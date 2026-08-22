param(
    [string]$RepoName = "ain-h8-public-signal-journey-review"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw "git is not installed or not on PATH."
}
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI (gh) is not installed. Use PUBLISH_TO_EXISTING_REPO.ps1 after creating an empty public repository in GitHub."
}

gh auth status | Out-Host

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (Test-Path .git) { Remove-Item -Recurse -Force .git }

git init | Out-Host
git checkout -b main | Out-Host
git add .
git commit -m "Public sanitized H8 signal journey review mirror" | Out-Host

gh repo create $RepoName --public --source . --remote origin --push

Write-Host ""
Write-Host "Published public review repository:"
gh repo view --json url --jq .url
