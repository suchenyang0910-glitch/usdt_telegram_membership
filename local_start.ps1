$ErrorActionPreference = "Stop"

$envFile = Join-Path $PSScriptRoot "local_userbot.env"
if (-not (Test-Path $envFile)) {
  Write-Host "missing local_userbot.env"
  exit 1
}

Get-Content $envFile -Encoding UTF8 | ForEach-Object {
  if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
  $k,$v = $_ -split '=',2
  $key = $k.Trim().Trim([char]0xFEFF)
[Environment]::SetEnvironmentVariable($key, $v.Trim(), 'Process')
}

if ($env:LOCAL_UPLOADER_BASE_URL -and $env:LOCAL_UPLOADER_TOKEN) {
  Write-Host "start admin tasks (uploader + downloader)"
  $p1 = Start-Process -FilePath "python" -ArgumentList @(Join-Path $PSScriptRoot "local_uploader_userbot.py") -NoNewWindow -PassThru
  $p2 = Start-Process -FilePath "python" -ArgumentList @(Join-Path $PSScriptRoot "local_downloader.py") -NoNewWindow -PassThru
  try { $p2.PriorityClass = "BelowNormal" } catch {}
  try { $p2.ProcessorAffinity = $p2.ProcessorAffinity } catch {}
  Write-Host ("uploader pid=" + $p1.Id + " downloader pid=" + $p2.Id)
  Wait-Process -Id @($p1.Id)
} else {
  Write-Host "start legacy local userbot (local_userbot_single.py)"
  python (Join-Path $PSScriptRoot "local_userbot_single.py")
}


