param(
    [int]$TimeoutSeconds = 300,
    [int]$PollSeconds = 3,
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"

function Get-ContainerId {
    param([string]$Service)
    $raw = docker compose ps -a -q $Service
    if ($null -eq $raw) {
        return ""
    }
    $id = "$raw".Trim()
    return $id
}

function Get-ContainerState {
    param([string]$ContainerId)
    docker inspect $ContainerId --format "{{.State.Status}}"
}

function Get-ContainerHealth {
    param([string]$ContainerId)
    docker inspect $ContainerId --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}"
}

Write-Host "Starting stack..."
if ($NoBuild) {
    docker compose up -d
} else {
    docker compose up -d --build
}

$services = @(
    "postgres-airflow",
    "postgres-analytics",
    "kafka",
    "kafka-ui",
    "minio",
    "spark-master",
    "spark-worker",
    "airflow-webserver",
    "airflow-scheduler",
    "streamlit"
)

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)

while ((Get-Date) -lt $deadline) {
    $allReady = $true
    $statusLines = @()

    foreach ($svc in $services) {
        $cid = Get-ContainerId -Service $svc
        if (-not $cid) {
            $allReady = $false
            $statusLines += "{0,-18} missing" -f $svc
            continue
        }

        $state = Get-ContainerState -ContainerId $cid
        $health = Get-ContainerHealth -ContainerId $cid

        if ($state -ne "running") {
            $allReady = $false
        } elseif ($health -ne "none" -and $health -ne "healthy") {
            $allReady = $false
        }

        $statusLines += "{0,-18} state={1} health={2}" -f $svc, $state, $health
    }

    # airflow-init is one-shot and must exit successfully
    $initId = Get-ContainerId -Service "airflow-init"
    $initOk = $false
    if ($initId) {
        $initState = Get-ContainerState -ContainerId $initId
        $initExit = docker inspect $initId --format "{{.State.ExitCode}}"
        $initOk = ($initState -eq "exited" -and $initExit -eq "0")
        $statusLines += "{0,-18} state={1} exit={2}" -f "airflow-init", $initState, $initExit
    } else {
        $statusLines += "{0,-18} missing" -f "airflow-init"
    }

    if ($allReady -and $initOk) {
        Write-Host ""
        Write-Host "Smoke test passed."
        $statusLines | ForEach-Object { Write-Host $_ }
        exit 0
    }

    Write-Host ""
    Write-Host "Waiting for services..."
    $statusLines | ForEach-Object { Write-Host $_ }
    Start-Sleep -Seconds $PollSeconds
}

Write-Host ""
Write-Host "Smoke test failed after $TimeoutSeconds seconds."
docker compose ps
exit 1
