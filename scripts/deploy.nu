use lib.nu [fail]
use std/log

const RETRYABLE_ERRORS = [timeout connection network 'read timed out']
const ZERO_SHA = '0000000000000000000000000000000000000000'

# Write one output for the next job of the workflow, or print it when run by hand.
def write-output [name: string, value: string]: nothing -> nothing {
    let line = $'($name)=($value)'

    match $env.GITHUB_OUTPUT? {
        null => { print $line }
        $file => {
            try {
                $'($line)(char nl)' | save --append $file
            } catch {|err| fail $'Cannot write the output ($name): ($err.msg)' {command: write-output, span: (metadata $name).span} }
        }
    }
}

# Return the deployments of a pipeline for one environment. A pipeline without a `prefect.yaml` has none.
def find-deployments [pipeline: string, environment: string]: nothing -> list<string> {
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

# Return the pipelines whose files changed between two commits. Without a `before` commit, use the parent of `after`.
def changed-pipelines [before: oneof<string, nothing>, after: string]: nothing -> list<string> {
    let base = if ($before == null) or ($before == '') or ($before == $ZERO_SHA) { $'($after)^' } else { $before }

    git diff --name-only $base $after -- pipelines
    | lines
    | each { path split | get --optional 1 }
    | where $it != null
    | uniq
}

# Tell whether a branch has an open pull request into master.
def has-open-pr [branch: string]: nothing -> bool {
    let result = gh pr list --head $branch --base master --state open --json number | complete
    if $result.exit_code != 0 {
        fail $'Cannot list the pull requests: ($result.stderr)' {command: has-open-pr, span: (metadata $branch).span}
    }

    let pulls = try {
        $result.stdout | from json
    } catch {|err| fail $'Cannot read the pull requests: ($err.msg)' {command: has-open-pr, span: (metadata $branch).span} }

    ($pulls | length) > 0
}

# Choose the pipelines to deploy and write the matrix of the deploy job.
def 'main plan' [
    --environment: string # `staging` or `prod`
    --pipeline: string # deploy only this pipeline
    --all # deploy every pipeline
    --before: string # the commit before the push
    --after: string = HEAD # the commit of the push
    --open-pr-from: string # deploy only if this branch has an open pull request into master
]: nothing -> nothing {
    if ($open_pr_from != null) and not (has-open-pr $open_pr_from) {
        log warning $'No open pull request from ($open_pr_from) into master, so nothing is deployed'
        write-output deploy 'false'
        write-output matrix ({
            pipeline: []
        } | to json --raw)
        return
    }

    let candidates = if $pipeline != null {
        [$pipeline]
    } else if $all {
        glob pipelines/*/prefect.yaml | each { path dirname | path basename }
    } else {
        changed-pipelines $before $after
    }

    let chosen = $candidates | where (find-deployments $it $environment | is-not-empty) | sort

    if ($pipeline != null) and ($chosen | is-empty) {
        fail $'The pipeline ($pipeline) has no ($environment) deployment' {command: plan, span: (metadata $pipeline).span}
    }

    log info $'Pipelines to deploy to ($environment): ($chosen | length)'
    write-output deploy (($chosen | is-not-empty) | into string)
    write-output matrix ({pipeline: $chosen} | to json --raw)
}

# Register the deployment of one pipeline in Prefect, and try again after a network error.
def 'main run' [
    pipeline: string # the pipeline folder name
    --environment: string # `staging` or `prod`
    --retries: int = 2 # how many times to try again
    --delay: duration = 30sec # the wait before the first new try, doubled each time
    --dry-run # print the command and stop
]: nothing -> nothing {
    let deployment = find-deployments $pipeline $environment | get --optional 0
    if $deployment == null {
        fail $'The pipeline ($pipeline) has no ($environment) deployment' {command: run, span: (metadata $pipeline).span}
    }

    let file = $'pipelines/($pipeline)/prefect.yaml'
    let command = [
        uv
        run
        --package
        $pipeline
        --
        prefect
        --no-prompt
        deploy
        --name
        $deployment
        --prefect-file
        $file
    ]

    if $dry_run {
        print ($command | str join ' ')
        return
    }

    mut wait = $delay
    for attempt in 0..$retries {
        log info $'Deploying ($deployment), attempt ($attempt + 1)'
        let result = run-external ...$command | complete

        if ($result.stdout | is-not-empty) { print $result.stdout }
        if $result.exit_code == 0 {
            log info $'Deployed ($deployment)'
            return
        }

        let retryable = $RETRYABLE_ERRORS | any {|word| $result.stderr | str lowercase | str contains $word }
        if (not $retryable) or $attempt == $retries {
            fail $'Could not deploy ($deployment): ($result.stderr)' {command: run, span: (metadata $pipeline).span}
        }

        log warning $'Network error, trying again in ($wait): ($result.stderr)'
        sleep $wait
        $wait *= 2
    }
}

# List the deploy commands.
def main []: nothing -> nothing {
    help main
}
