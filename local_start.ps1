$ErrorActionPreference = "Stop"

$envFile = Join-Path $PSScriptRoot "local_userbot.env"
if (-not (Test-Path $envFile)) {
  Write-Host "missing local_userbot.env"
  exit 1
}

Get-Content $envFile -Encoding UTF8 | ForEach-Object {
  if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
  $k,$v = $_ -split '=',2
  [Environment]::SetEnvironmentVariable($k.Trim(), $v.Trim(), 'Process')
}

python (Join-Path $PSScriptRoot "local_userbot_pool.py")

