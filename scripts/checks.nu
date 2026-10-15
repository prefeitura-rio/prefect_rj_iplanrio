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
    in-project . pyrefly check
    in-project packages/toolkit ruff check .
    in-project packages/toolkit ruff format --check .
    in-project packages/toolkit basedpyright
    in-project packages/toolkit pyrefly check
    in-project packages/toolkit vulture toolkit tests
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

# Run the unit tests of the root project. They need no network.
def 'main test root' []: nothing -> nothing {
    in-project . pytest -q
}

# Run the unit tests of the toolkit. They need no network.
def 'main test toolkit' []: nothing -> nothing {
    in-project packages/toolkit pytest -q
}
