<#
GraphPulse demo helper for a Windows laptop. Use it together with cluster/gcp/demo.sh in Cloud Shell.
  1. Finds this laptop's public IP and copies the Cloud Shell start command to the clipboard.
  2. Asks for MASTER IP (Cloud Shell prints it at the end) and points the name "master" at it in the
     hosts file (Windows asks for permission), so http://master:8000 and the links in the Hadoop pages work.
  3. Checks that the dashboard answers and opens the dashboard, YARN, HDFS and HBase pages.
Start it by double-clicking demo_laptop.cmd, which also works where PowerShell scripts are blocked.
#>
param(
  [string]$MasterIp,                                            # skip the questions if you know it
  [switch]$SetHosts,                                            # internal: the admin copy that edits hosts
  [string]$HostsFile = "$env:SystemRoot\System32\drivers\etc\hosts"
)
$ErrorActionPreference = 'Stop'

function Test-IPv4([string]$s) { return [bool]($s -match '^(\d{1,3}\.){3}\d{1,3}$') }

function Set-MasterHost([string]$Path, [string]$Ip) {
  # Drop any older "<ip> master" line, keep every other line exactly as it is, add the new one.
  $keep = @(Get-Content -Path $Path | Where-Object { $_ -notmatch '^\s*[0-9A-Fa-f:.]+\s+master\s*(#.*)?$' })
  Set-Content -Path $Path -Value ($keep + "$Ip master  # graphpulse") -Encoding Default
}

function Test-MasterHost([string]$Path, [string]$Ip) {
  $pattern = '^\s*' + [regex]::Escape($Ip) + '\s+master\s*(#.*)?$'
  return [bool](Get-Content -Path $Path -ErrorAction SilentlyContinue | Where-Object { $_ -match $pattern })
}

if ($SetHosts) {
  Set-MasterHost -Path $HostsFile -Ip $MasterIp
  if ($HostsFile -like "$env:SystemRoot*") { ipconfig /flushdns | Out-Null }
  exit 0
}

Write-Host ''
Write-Host 'GraphPulse demo helper' -ForegroundColor Cyan
$myIp = $null
if (-not $MasterIp) {
  try { $myIp = ([string](Invoke-RestMethod -Uri 'https://api.ipify.org' -TimeoutSec 15)).Trim() } catch { }
  if (Test-IPv4 $myIp) {
    $cmd = "~/graphpulse/cluster/gcp/demo.sh start $myIp"
    Set-Clipboard -Value $cmd
    Write-Host "1) This laptop's public IP is $myIp. The Cloud Shell command is on your clipboard:"
    Write-Host "     $cmd" -ForegroundColor Yellow
    Write-Host '   Paste it into Cloud Shell (Ctrl+V) and press Enter.'
  } else {
    Write-Host "1) Could not find this laptop's public IP. Open https://api.ipify.org and run in Cloud Shell:"
    Write-Host '     ~/graphpulse/cluster/gcp/demo.sh start YOUR_IP' -ForegroundColor Yellow
  }
  Write-Host '   Wait until Cloud Shell prints DEMO READY and MASTER IP (about 5 minutes).'
  do { $MasterIp = ([string](Read-Host '2. Paste MASTER IP here')).Trim() } until (Test-IPv4 $MasterIp)
}

$base = "http://$MasterIp"
if (-not (Test-MasterHost -Path $HostsFile -Ip $MasterIp)) {
  Write-Host "3) Pointing the name 'master' at $MasterIp. Click Yes in the Windows prompt."
  try {
    Start-Process -FilePath 'powershell.exe' -Verb RunAs -Wait -WindowStyle Hidden -ArgumentList @(
      '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', "`"$PSCommandPath`"", '-SetHosts', '-MasterIp', $MasterIp)
  } catch { }
}
if (Test-MasterHost -Path $HostsFile -Ip $MasterIp) {
  $base = 'http://master'
  Write-Host "   OK: http://master now goes to $MasterIp" -ForegroundColor Green
} else {
  Write-Host "   Not changed, so the pages open by IP. A few links inside the Hadoop pages won't open." -ForegroundColor DarkYellow
}

Write-Host '4) Checking the dashboard...'
try {
  $h = Invoke-RestMethod -Uri "${base}:8000/api/health" -TimeoutSec 10
  Write-Host ("   OK: data from {0}, live events from {1}" -f $h.backend, $h.live.mode) -ForegroundColor Green
} catch {
  Write-Host "   The dashboard does not answer at ${base}:8000 yet." -ForegroundColor Red
  Write-Host '   - Wait for DEMO READY in Cloud Shell, then run this helper again.'
  Write-Host '   - If this laptop changed network, its IP changed: run the helper again and use the new start command.'
}
foreach ($port in 8000, 8088, 9870, 16010) { Start-Process "${base}:$port" }
Write-Host 'Opened the dashboard, YARN, HDFS and HBase pages. When you are done, run in Cloud Shell:'
Write-Host '     ~/graphpulse/cluster/gcp/demo.sh stop' -ForegroundColor Yellow
