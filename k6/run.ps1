[CmdletBinding()]
param(
    [ValidateSet('smoke', 'load', 'reliability')]
    [string]$Test = 'smoke',

    [string]$BaseUrl = 'http://localhost:8080',

    [switch]$Docker
)

$ErrorActionPreference = 'Stop'
$scriptFile = "$Test.js"

if ($Docker) {
    $dockerBaseUrl = $BaseUrl -replace '^http://localhost', 'http://host.docker.internal'

    docker run --rm `
        -e "BASE_URL=$dockerBaseUrl" `
        -e "RATE=$env:RATE" `
        -e "DURATION=$env:DURATION" `
        -e "PRE_ALLOCATED_VUS=$env:PRE_ALLOCATED_VUS" `
        -e "MAX_VUS=$env:MAX_VUS" `
        -e "SETTLE_SECONDS=$env:SETTLE_SECONDS" `
        -e "VERIFY_ATTEMPTS=$env:VERIFY_ATTEMPTS" `
        -e "VERIFY_INTERVAL_SECONDS=$env:VERIFY_INTERVAL_SECONDS" `
        -v "${PSScriptRoot}:/scripts:ro" `
        grafana/k6:2.1.0 run "/scripts/$scriptFile"

    exit $LASTEXITCODE
}

$env:BASE_URL = $BaseUrl
k6 run (Join-Path $PSScriptRoot $scriptFile)
exit $LASTEXITCODE
