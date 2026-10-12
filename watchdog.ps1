$ErrorActionPreference = 'Stop'
$proxyRoot = 'D:\Biblioteca\Descargas\nim-rotator-proxy'
$logPath = Join-Path $proxyRoot 'data\watchdog.log'
$pythonw = 'C:\Python313\pythonw.exe'
$mutex = [System.Threading.Mutex]::new($false, 'Local\NimRotatorProxyWatchdog')

if (-not $mutex.WaitOne(0)) { exit 0 }

try {
    $lastState = ''
    while ($true) {
        try {
            $healthy = $false
            try {
                $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8377/health' -TimeoutSec 4
                $healthy = ($health.ok -eq $true -and $health.service -eq 'nim-rotator-proxy')
            } catch {}

            if ($healthy) {
                if ($lastState -and $lastState -ne 'healthy') {
                    Add-Content -LiteralPath $logPath -Value ('{0} health restored' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')) -Encoding UTF8
                }
                $lastState = 'healthy'
            } else {
                $listeners = @(Get-NetTCPConnection -LocalPort 8377 -State Listen -ErrorAction SilentlyContinue)
                if ($listeners.Count -gt 0) {
                    if ($lastState -ne 'unhealthy-listener') {
                        Add-Content -LiteralPath $logPath -Value ('{0} health endpoint failed but port is owned by pid {1}; skipped duplicate launch' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $listeners[0].OwningProcess) -Encoding UTF8
                    }
                    $lastState = 'unhealthy-listener'
                } else {
                    if (-not (Test-Path -LiteralPath $pythonw)) { throw "pythonw.exe not found at $pythonw" }
                    $process = Start-Process -FilePath $pythonw -ArgumentList @('proxy.py') -WorkingDirectory $proxyRoot -WindowStyle Hidden -PassThru
                    if ($lastState -ne 'starting') {
                        Add-Content -LiteralPath $logPath -Value ('{0} started proxy pid {1}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $process.Id) -Encoding UTF8
                    }
                    $lastState = 'starting'
                }
            }
        } catch {
            Add-Content -LiteralPath $logPath -Value ('{0} watchdog error: {1}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $_.Exception.Message) -Encoding UTF8
        }
        Start-Sleep -Seconds 15
    }
} finally {
    $mutex.ReleaseMutex()
    $mutex.Dispose()
}
