param(
    [Parameter(Mandatory=$true)]
    [string]$RepoUrl
)

$ErrorActionPreference = "Stop"
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw "git is not installed or not on PATH."
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (Test-Path .git) { Remove-Item -Recurse -Force .git }

git init | Out-Host
git checkout -b main | Out-Host
git add .
git commit -m "Public sanitized H8 signal journey review mirror" | Out-Host
git remote add origin $RepoUrl
git push -u origin main

Write-Host "Published to $RepoUrl"
