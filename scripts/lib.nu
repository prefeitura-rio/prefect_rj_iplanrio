use std/log

# Log an error and raise a labeled error in one call.
export def fail [message: string, context: record<command: string, span: record>]: nothing -> error {
    log error $message
    error make {
        msg: $message
        label: {text: $context.command, span: $context.span}
    }
}

const ALLOW_FILE = '.allow.json'

# Return the deployments of a pipeline for one environment. A pipeline without a `prefect.yaml` has none.
export def find-deployments [pipeline: string, environment: string]: nothing -> list<string> {
    let file = $'pipelines/($pipeline)/prefect.yaml'
    if not ($file | path exists) { return [] }

    let config = try { open $file } catch {|err|
        fail $'Cannot read ($file): ($err.msg)' {command: find-deployments, span: (metadata $file).span}
    }

    $config
    | get --optional deployments
    | default []
    | each {|deployment| $deployment.name? }
    | compact
    | where ($it | str ends-with $'--($environment)')
}

# Return the pipelines that may deploy to one environment, from `.allow.json`.
export def read-allowed [environment: string]: nothing -> list<string> {
    let allowed = try { open $ALLOW_FILE } catch {|err|
        fail $'Cannot read ($ALLOW_FILE): ($err.msg)' {command: read-allowed, span: (metadata $environment).span}
    }

    let names = try { $allowed | get $environment } catch {
        fail $'($ALLOW_FILE) has no list for ($environment)' {command: read-allowed, span: (metadata $environment).span}
    }

    if not (($names | describe) starts-with 'list') {
        fail $'The ($environment) entry of ($ALLOW_FILE) is not a list' {command: read-allowed, span: (metadata $environment).span}
    }

    $names
}
