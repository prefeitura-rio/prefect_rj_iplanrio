use lib.nu [fail]

# Run one command in a folder and fail with its name if it exits with an error.
def --wrapped in-project [folder: path, command: string, ...arguments: string]: nothing -> nothing {
    let result = ^uv run --frozen --no-sync --directory $folder $command ...$arguments | complete

    if ($result.stdout | is-not-empty) { print $result.stdout }

    if $result.exit_code != 0 {
        fail $'($command) failed in ($folder): ($result.stderr)' {command: in-project, span: (metadata $command).span}
    }
}

# List the checks.
def main []: nothing -> nothing {
    help main
}

# Install the root project and the toolkit with their development tools.
def 'main sync' []: nothing -> nothing {
    ^uv sync --frozen
    ^uv sync --frozen --package toolkit --inexact
}

# Check style and types in the root project and in the toolkit.
def 'main lint python' []: nothing -> nothing {
    in-project . ruff check src tests
    in-project . ruff format --check src tests
    in-project . basedpyright
    in-project packages/toolkit ruff check .
    in-project packages/toolkit ruff format --check .
    in-project packages/toolkit basedpyright
    in-project packages/toolkit vulture toolkit tests
}

# Fail if a pipeline imports a module that no longer exists.
def 'main lint imports' []: nothing -> nothing {
    in-project . ruff check --select TID251 --no-fix pipelines
}

# Fail when a setting breaks the naming rule or a pipeline reads an environment variable by hand.
def 'main lint env' []: nothing -> nothing {
    let naming = '^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$'
    let settings = glob pipelines/*/settings.py | append ['src/utils/settings.py' 'packages/toolkit/toolkit/settings.py']
    let misnamed = $settings | each {|file|
        open --raw $file
        | lines
        | parse --regex '^    (?<name>[A-Za-z_][A-Za-z0-9_]*):'
        | get name
        | where {|name| $name != 'model_config' and not ($name =~ $naming) }
        | each {|name| $'($file): ($name) is not UPPER_SNAKE_CASE with single underscores' }
    } | flatten

    let hand_read = (glob pipelines/**/*.py) | append (glob src/**/*.py) | where {|file| not ($file | str contains '/tests/') } | each {|file|
        open --raw $file
        | lines
        | enumerate
        | where {|row|
            let line = $row.item
            ($line =~ 'getenv\(|environ\.get\(|read_env\(') or (($line =~ 'environ\[') and not ($line =~ '\] =') and not ($line | str trim --left | str starts-with 'del '))
        }
        | each {|row| $'($file):($row.index + 1): read the variable through a Settings class' }
    } | flatten

    let problems = $misnamed | append $hand_read
    if ($problems | is-not-empty) {
        fail $'Fix the environment variables:(char nl)($problems | str join (char nl))' {command: 'lint env', span: (metadata $problems).span}
    }
}

# Fail when a pipeline imports nothing that starts the telemetry, so its runs would send no labels.
def 'main lint telemetry' []: nothing -> nothing {
    let starters = '^(from|import) (utils\.(tasks|utils|flows|hooks)|toolkit\.(telemetry|gcs|dbt|executor|oracle|delta|batches|chunks|bigquery))'
    let silent = ls pipelines | where type == dir | get name | where {|folder|
        let files = glob $'($folder)/**/*.py' | where {|file| not ($file | str contains '/tests/') }
        $files | all {|file| not (open --raw $file | lines | any {|line| $line =~ $starters }) }
    } | each {|folder| $'($folder): import toolkit.telemetry, utils.tasks, utils.utils or utils.flows' }

    if ($silent | is-not-empty) {
        fail $'Make these pipelines send telemetry:(char nl)($silent | str join (char nl))' {command: 'lint telemetry', span: (metadata $silent).span}
    }
}

# Lint the SQL templates of the toolkit.
def 'main lint sql' []: nothing -> nothing {
    in-project packages/toolkit sqlfluff lint toolkit/executor/templates
}

# Read a TOML file, or fail with its path.
def read-toml [file: path]: nothing -> record {
    try { open $file } catch {|err| fail $'Cannot read ($file): ($err.msg)' {command: read-toml, span: (metadata $file).span} }
}

# Return the name of a requirement, such as `pendulum` for `pendulum==3.0.0`, in the form that pip compares.
def requirement-name []: string -> string {
    str replace --regex '[^A-Za-z0-9_.-].*$' '' | str lowercase | str replace --all '_' '-'
}

# Fail when a pipeline sets a version, or a git URL, for a package that the root already sets.
def 'main lint deps' []: nothing -> nothing {
    let root = read-toml pyproject.toml | get tool.uv
    let central = $root.constraint-dependencies | append ($root.sources | columns) | each { requirement-name }

    let violations = glob pipelines/*/pyproject.toml
    | each {|file|
        read-toml $file | get project.dependencies
        | where ($it | requirement-name) in $central and ($it | str replace --regex '^[A-Za-z0-9_.-]+(\[[^\]]*\])?' '' | str trim | is-not-empty)
        | each {|dependency| $'($file): ($dependency)' }
    }
    | flatten

    if ($violations | is-not-empty) {
        fail $'Set the version in the root pyproject.toml, not in a pipeline:(char nl)($violations | str join (char nl))' {command: 'lint deps', span: (metadata $violations).span}
    }
}

# Fail when a pipeline `prefect.yaml` has a comment or cannot be read by Nushell, which the deploy script uses.
def 'main lint prefect' []: nothing -> nothing {
    let files = glob pipelines/*/prefect.yaml
    let unreadable = $files | where (try { open $it | ignore; false } catch { true })
    let commented = $files | where (try { open --raw $it | lines | any {|line| $line | str trim --left | str starts-with '#' } } catch { true })
    let problems = $unreadable | each { $'($in): cannot be read' } | append ($commented | each { $'($in): has a comment' })

    if ($problems | is-not-empty) {
        fail $'Fix the prefect.yaml files:(char nl)($problems | str join (char nl))' {command: 'lint prefect', span: (metadata $problems).span}
    }
}

# Run the unit tests of the root project on four workers. They need no network.
def 'main test root' []: nothing -> nothing {
    in-project . pytest -q -n 4 --dist worksteal
}

# Run the unit tests of the toolkit. They need no network.
def 'main test toolkit' []: nothing -> nothing {
    in-project packages/toolkit pytest -q
}
