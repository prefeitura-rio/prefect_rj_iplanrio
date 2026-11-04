# The parity lab: six real engines under $env.PARITY_LAB, on random ports.
#
#   nu tests/parity/engines.nu probe
#   nu tests/parity/engines.nu up postgres mysql couchdb mongodb mssql oracle
#   nu tests/parity/engines.nu status
#   nu tests/parity/engines.nu down
#   nu tests/parity/engines.nu clean
#
# `MONGODB_IMAGE=docker.io/library/mongo:8.0 nu tests/parity/engines.nu up mongodb` runs MongoDB in a container
# instead of the nix package. Use another PARITY_LAB to keep two versions apart.
#
# Then run `uv run pytest -m parity tests/parity`. `PARITY_LAB` is /tmp/toolkit-parity unless
# it is set. Nothing here touches a path outside it, or a container that does not start with
# `engines-`. Passwords are generated, kept in env.json (mode 600) and never printed.

const HERE = (path self | path dirname)
const ALL_PATHS = ($HERE | path join ".." ".." | path expand)
const ALL = [postgres mysql couchdb mongodb mssql oracle]
const MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
const ORACLE_IMAGE = "docker.io/gvenzl/oracle-free:23-slim-faststart"

def lab-dir []: nothing -> string { $env.PARITY_LAB? | default "/tmp/toolkit-parity" }

def env-file []: nothing -> string { $"(lab-dir)/env.json" }

def load-env []: nothing -> record {
    if (env-file | path exists) { open (env-file) } else { {} }
}

def save-env [name: string, entry: record]: nothing -> nothing {
    let merged = (load-env | upsert $name $entry)
    $merged | to json | save --force (env-file)
    chmod 600 (env-file)
}

def free-port []: nothing -> int {
    ^python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])' | str trim | into int
}

def secret []: nothing -> string { ^openssl rand -hex 10 | str trim }

# The store path of a nix package that has a `bin` directory.
def nix-bin [attr: string]: nothing -> string {
    with-env {NIXPKGS_ALLOW_UNFREE: "1"} {
        ^nix build --impure --no-link --print-out-paths $"nixpkgs#($attr)" e> /dev/null
        | lines
        | where {|path| ($path | path join "bin" | path exists) }
        | first
    }
}

def background [command: string, log: string, pidfile: string]: nothing -> nothing {
    ^sh -c $"nohup ($command) > ($log) 2>&1 & echo $! > ($pidfile)"
}

def wait-for [label: string, check: closure, seconds: int = 120] {
    for _ in 1..$seconds {
        if (do $check) { return }
        sleep 1sec
    }
    error make {msg: $"($label) did not become ready in ($seconds) s"}
}

def pid-of [dir: string]: nothing -> any {
    let file = ($dir | path join "pid")
    if ($file | path exists) { open $file | str trim | into int } else { null }
}

# ---------------------------------------------------------------- probe

def "main probe" []: nothing -> table {
    [podman nix openssl python3 sh] | each {|tool|
        {tool: $tool, found: (which $tool | is-not-empty)}
    }
}

# ---------------------------------------------------------------- up

def "main up" [...names: string]: nothing -> nothing {
    let wanted = if ($names | is-empty) { $ALL } else { $names }
    mkdir (lab-dir)
    for name in $wanted {
        print $"up ($name)"
        match $name {
            "postgres" => { up-postgres }
            "mysql" => { up-mysql }
            "couchdb" => { up-couchdb }
            "mongodb" => { up-mongodb }
            "mssql" => { up-mssql }
            "oracle" => { up-oracle }
            _ => { error make {msg: $"unknown engine ($name)"} }
        }
    }
}

def up-postgres []: nothing -> nothing {
    let bin = (nix-bin "postgresql_17") | path join "bin"
    let dir = $"(lab-dir)/postgres"
    let port = (free-port)
    let password = (secret)
    rm -rf $dir
    mkdir $dir
    $password | save --force $"($dir)/pw"
    ^$"($bin)/initdb" -D $"($dir)/data" -U lab --auth=scram-sha-256 $"--pwfile=($dir)/pw" e> $"($dir)/initdb.log" | ignore
    ^$"($bin)/pg_ctl" -D $"($dir)/data" -o $"-p ($port) -k ($dir) -c listen_addresses=127.0.0.1" -l $"($dir)/log" -w start | ignore
    with-env {PGPASSWORD: $password} {
        ^$"($bin)/createdb" -h 127.0.0.1 -p $port -U lab lab
        ^python3 $"($HERE)/parity_dataset.py" postgres | save --force $"($dir)/seed.sql"
        ^$"($bin)/psql" -h 127.0.0.1 -p $port -U lab -d lab -v ON_ERROR_STOP=1 -q -f $"($dir)/seed.sql" | ignore
    }
    save-env postgres {
        host: "127.0.0.1", port: $port, database: "lab", user: "lab", password: $password,
        table: "public.orders", bin: $bin,
    }
}

def up-mysql []: nothing -> nothing {
    let root = (nix-bin "mysql84")
    let bin = $root | path join "bin"
    let dir = $"(lab-dir)/mysql"
    let port = (free-port)
    let password = (secret)
    rm -rf $dir
    mkdir $dir
    ^$"($bin)/mysqld" --no-defaults --initialize-insecure $"--basedir=($root)" $"--datadir=($dir)/data" e> $"($dir)/init.log" | ignore
    let command = $"($bin)/mysqld --no-defaults --basedir=($root) --datadir=($dir)/data --port=($port) --bind-address=127.0.0.1 --socket=($dir)/mysql.sock --mysqlx=OFF --mysql-native-password=ON --pid-file=($dir)/mysqld.pid"
    background $command $"($dir)/log" $"($dir)/pid"
    wait-for "mysql" {|| (do -i { ^$"($bin)/mysqladmin" --no-defaults -uroot -S $"($dir)/mysql.sock" ping e> /dev/null | complete | get exit_code } | default 1) == 0 }
    let setup = $"CREATE DATABASE lab CHARACTER SET utf8mb4; CREATE USER 'lab'@'%' IDENTIFIED WITH mysql_native_password BY '($password)'; GRANT ALL ON lab.* TO 'lab'@'%';"
    ^$"($bin)/mysql" --no-defaults -uroot -S $"($dir)/mysql.sock" -e $setup
    ^python3 $"($HERE)/parity_dataset.py" mysql | save --force $"($dir)/seed.sql"
    with-env {MYSQL_PWD: $password} {
        ^sh -c $"($bin)/mysql --no-defaults -h127.0.0.1 -P($port) -ulab lab < ($dir)/seed.sql"
    }
    save-env mysql {
        host: "127.0.0.1", port: $port, database: "lab", user: "lab", password: $password,
        table: "lab.orders", bin: $bin,
    }
}

def up-couchdb []: nothing -> nothing {
    let root = (nix-bin "couchdb3")
    let dir = $"(lab-dir)/couchdb"
    let port = (free-port)
    let password = (secret)
    rm -rf $dir
    mkdir $"($dir)/data"
    $"[couchdb]\nsingle_node = true\n\n[cluster]\nn = 1\n\n[couchdb]\ndatabase_dir = ($dir)/data\nview_index_dir = ($dir)/data\nuri_file = ($dir)/couch.uri\n\n[chttpd]\nport = ($port)\nbind_address = 127.0.0.1\n\n[admins]\nlab = ($password)\n\n[log]\nwriter = file\nfile = ($dir)/couch.log\nlevel = warning\n" | save --force $"($dir)/local.ini"
    let command = $"($root)/bin/couchdb -couch_ini ($root)/etc/default.ini ($dir)/local.ini"
    background $command $"($dir)/log" $"($dir)/pid"
    wait-for "couchdb" {|| (^curl -s -o /dev/null -w "%{http_code}" $"http://127.0.0.1:($port)/_up" | complete | get stdout | str trim) == "200" } 180
    for system in [_users _replicator] {
        ^curl -s -o /dev/null -X PUT -u $"lab:($password)" $"http://127.0.0.1:($port)/($system)"
    }
    save-env couchdb {
        host: "127.0.0.1", port: $port, database: "orders", user: "lab", password: $password,
        table: "orders",
    }
    with-env {PARITY_LAB: (lab-dir)} { ^$"($ALL_PATHS)/.venv/bin/python" $"($HERE)/parity_docs.py" couchdb }
}

def up-mongodb []: nothing -> nothing {
    let image = ($env.MONGODB_IMAGE? | default "")
    let dir = $"(lab-dir)/mongodb"
    let port = (free-port)
    let password = (secret)
    rm -rf $dir
    mkdir $"($dir)/data"
    if ($image | is-empty) {
        let bin = (nix-bin "mongodb-ce") | path join "bin"
        background $"($bin)/mongod --dbpath ($dir)/data --port ($port) --bind_ip 127.0.0.1 --auth --nounixsocket" $"($dir)/log" $"($dir)/pid"
    } else {
        ^podman run -d --name $"engines-mongodb-($port)" -e MONGO_INITDB_ROOT_USERNAME=lab -e $"MONGO_INITDB_ROOT_PASSWORD=($password)" -p $"127.0.0.1:($port):27017" $image | ignore
    }
    save-env mongodb {
        host: "127.0.0.1", port: $port, database: "lab", user: "lab", password: $password,
        table: "orders", auth_source: "admin", image: $image, container: (if ($image | is-empty) { "" } else { $"engines-mongodb-($port)" }),
    }
    with-env {PARITY_LAB: (lab-dir)} { ^$"($ALL_PATHS)/.venv/bin/python" $"($HERE)/parity_docs.py" mongodb }
}

def up-mssql []: nothing -> nothing {
    let port = (free-port)
    let password = $"Aa1-(secret)"
    ^podman rm -f engines-mssql e> /dev/null | ignore
    ^podman run -d --name engines-mssql -e ACCEPT_EULA=Y -e $"MSSQL_SA_PASSWORD=($password)" -p $"127.0.0.1:($port):1433" $MSSQL_IMAGE | ignore
    let sqlcmd = ["/opt/mssql-tools18/bin/sqlcmd" "-C" "-S" "localhost" "-U" "sa"]
    wait-for "mssql" {|| (do -i { ^podman exec -e $"SQLCMDPASSWORD=($password)" engines-mssql ...$sqlcmd -Q "SELECT 1" | complete | get exit_code } | default 1) == 0 } 240
    ^podman exec -e $"SQLCMDPASSWORD=($password)" engines-mssql ...$sqlcmd -b -Q "CREATE DATABASE lab" | ignore
    ^python3 $"($HERE)/parity_dataset.py" mssql | save --force $"(lab-dir)/mssql-seed.sql"
    ^podman cp $"(lab-dir)/mssql-seed.sql" engines-mssql:/tmp/seed.sql
    ^podman exec -e $"SQLCMDPASSWORD=($password)" engines-mssql ...$sqlcmd -b -d lab -i /tmp/seed.sql | ignore
    save-env mssql {
        host: "127.0.0.1", port: $port, database: "lab", user: "sa", password: $password,
        table: "dbo.orders",
    }
}

def up-oracle []: nothing -> nothing {
    let port = (free-port)
    let password = (secret)
    ^podman rm -f engines-oracle e> /dev/null | ignore
    ^podman run -d --name engines-oracle -e $"ORACLE_PASSWORD=($password)" -e APP_USER=lab -e $"APP_USER_PASSWORD=($password)" -p $"127.0.0.1:($port):1521" $ORACLE_IMAGE | ignore
    wait-for "oracle" {|| (^podman logs engines-oracle o+e>| str contains "DATABASE IS READY TO USE") } 480
    let grants = "ALTER SESSION SET CONTAINER = FREEPDB1;\nGRANT EXECUTE ON SYS.DBMS_FLASHBACK TO lab;\nGRANT SELECT ON SYS.V_$DATABASE TO lab;\nGRANT FLASHBACK ANY TABLE TO lab;\nEXIT;\n"
    $grants | save --force $"(lab-dir)/oracle-grants.sql"
    ^podman cp $"(lab-dir)/oracle-grants.sql" engines-oracle:/tmp/grants.sql
    ^podman exec engines-oracle sqlplus -S "/ as sysdba" @/tmp/grants.sql | ignore
    ^python3 $"($HERE)/parity_dataset.py" oracle | save --force $"(lab-dir)/oracle-seed.sql"
    ^podman cp $"(lab-dir)/oracle-seed.sql" engines-oracle:/tmp/seed.sql
    ^podman exec engines-oracle sqlplus -S $"lab/($password)@//localhost:1521/FREEPDB1" @/tmp/seed.sql | ignore
    save-env oracle {
        host: "127.0.0.1", port: $port, database: "FREEPDB1", user: "lab", password: $password,
        table: "LAB.ORDERS",
    }
}

# ---------------------------------------------------------------- status, down, clean

def "main status" []: nothing -> table {
    let known = (load-env)
    $ALL | each {|name|
        let entry = ($known | get -o $name)
        {
            engine: $name
            port: ($entry | get -o port | default null)
            up: ($entry != null)
        }
    }
}

def "main down" []: nothing -> nothing {
    let bin = (load-env | get -o postgres | get -o bin)
    if $bin != null {
        do -i { ^$"($bin)/pg_ctl" -D $"(lab-dir)/postgres/data" -m fast stop | ignore }
    }
    for name in [mysql couchdb mongodb] {
        let pid = (pid-of $"(lab-dir)/($name)")
        if $pid != null { do -i { ^kill $pid } }
    }
    let mongodb = (load-env | get -o mongodb | get -o container | default "")
    for container in [engines-mssql engines-oracle $mongodb] {
        if ($container | is-empty) { continue }
        do -i { ^podman rm -f $container e> /dev/null | ignore }
    }
}

def "main clean" []: nothing -> nothing {
    main down
    sleep 3sec
    for name in [postgres mysql couchdb mongodb] { rm -rf $"(lab-dir)/($name)" }
    rm -f (env-file)
}

def main []: nothing -> nothing {
    print "probe | up [names] | status | down | clean"
}
