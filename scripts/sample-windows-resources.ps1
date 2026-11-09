param(
    [Parameter(Mandatory = $true)][string]$OutputPath,
    [Parameter(Mandatory = $true)][string]$StopFile,
    [Parameter(Mandatory = $true)][string]$WorkerProcessName,
    [ValidateRange(1, 60)][int]$IntervalSeconds = 2
)

$ErrorActionPreference = 'Stop'
$logicalProcessors = [Environment]::ProcessorCount
$previousCpuSeconds = $null
$previousSampleAt = $null

# Write a header even if the worker finishes before the first interval.
if (-not (Test-Path -LiteralPath $OutputPath)) {
    'timestamp,system_cpu_percent,available_memory_mb,total_memory_mb,worker_cpu_percent_of_total_capacity,worker_working_set_mb,worker_process_count,logical_processors' |
        Set-Content -LiteralPath $OutputPath -Encoding UTF8
}

while (-not (Test-Path -LiteralPath $StopFile)) {
    $sampleAt = Get-Date
    $os = Get-CimInstance -ClassName Win32_OperatingSystem
    $processor = Get-CimInstance -ClassName Win32_PerfFormattedData_PerfOS_Processor -Filter "Name='_Total'" -ErrorAction SilentlyContinue
    $systemCpu = if ($null -ne $processor) { [double]$processor.PercentProcessorTime } else { 0.0 }

    $workers = @(Get-Process -Name $WorkerProcessName -ErrorAction SilentlyContinue)
    $workerCount = $workers.Count
    $workerCpuSeconds = 0.0
    $workerWorkingSetBytes = 0.0
    foreach ($process in $workers) {
        if ($null -ne $process.CPU) { $workerCpuSeconds += [double]$process.CPU }
        $workerWorkingSetBytes += [double]$process.WorkingSet64
    }

    $workerCpuPercent = 0.0
    if ($null -ne $previousCpuSeconds -and $null -ne $previousSampleAt) {
        $elapsed = ($sampleAt - $previousSampleAt).TotalSeconds
        if ($elapsed -gt 0 -and $logicalProcessors -gt 0) {
            $cpuDelta = [Math]::Max(0.0, $workerCpuSeconds - $previousCpuSeconds)
            # Normalize to total machine CPU capacity (0..100%), not one core (0..100%).
            $workerCpuPercent = 100.0 * $cpuDelta / ($elapsed * $logicalProcessors)
        }
    }

    $row = [pscustomobject]@{
        timestamp = $sampleAt.ToString('o')
        system_cpu_percent = [Math]::Round($systemCpu, 2)
        available_memory_mb = [Math]::Round(([double]$os.FreePhysicalMemory / 1024.0), 2)
        total_memory_mb = [Math]::Round(([double]$os.TotalVisibleMemorySize / 1024.0), 2)
        worker_cpu_percent_of_total_capacity = [Math]::Round($workerCpuPercent, 2)
        worker_working_set_mb = [Math]::Round(($workerWorkingSetBytes / 1MB), 2)
        worker_process_count = $workerCount
        logical_processors = $logicalProcessors
    }
    $row | Export-Csv -LiteralPath $OutputPath -Append -NoTypeInformation -Encoding UTF8

    $previousCpuSeconds = $workerCpuSeconds
    $previousSampleAt = $sampleAt
    Start-Sleep -Seconds $IntervalSeconds
}
