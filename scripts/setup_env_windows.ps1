param(
    [string]$Python = "python",
    [string]$VenvPath = ".venv-win",
    [switch]$SkipHadoopDownload
)

$ErrorActionPreference = "Stop"

Write-Host "Creating Windows Python virtual environment at $VenvPath"
& $Python -m venv $VenvPath

$VenvPython = Join-Path $VenvPath "Scripts\python.exe"

Write-Host "Installing Python requirements..."
& $VenvPython -m pip install --upgrade pip wheel setuptools
& $VenvPython -m pip install -r requirements.txt

if (-not $SkipHadoopDownload) {
    $HadoopBin = ".hadoop\bin"
    New-Item -ItemType Directory -Force -Path $HadoopBin | Out-Null

    $BaseUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
    $Files = @("winutils.exe", "hadoop.dll")

    foreach ($FileName in $Files) {
        $Target = Join-Path $HadoopBin $FileName
        if (-not (Test-Path $Target)) {
            Write-Host "Downloading $FileName"
            Invoke-WebRequest -Uri "$BaseUrl/$FileName" -OutFile $Target
        }
    }
}

Write-Host "Setup complete. Run:"
Write-Host "$VenvPython run_all_pipeline.py --skip-landing --strict"
