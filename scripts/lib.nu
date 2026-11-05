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

# Reject a production deployment from a GitHub ref other than master.
export def require-master-for-prod [environment: string]: nothing -> nothing {
    let ref = $env.GITHUB_REF?

    if $environment == 'prod' and $ref != null and $ref != 'refs/heads/master' {
        fail $'Only master deploys to prod, not ($ref)' {command: require-master-for-prod, span: (metadata $environment).span}
    }
}

# Return the pipelines that may deploy to one environment, from `.allow.json` in the root of the repository.
# Return nothing if the file does not exist: then every pipeline may deploy.
export def read-allowed [environment: string]: nothing -> oneof<list<string>, nothing> {
    if not ($ALLOW_FILE | path exists) { return null }

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
