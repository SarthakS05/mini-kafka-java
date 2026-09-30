$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$mainOutput = Join-Path $projectRoot "target/classes"
$testOutput = Join-Path $projectRoot "target/test-classes"

New-Item -ItemType Directory -Path $mainOutput -Force | Out-Null
New-Item -ItemType Directory -Path $testOutput -Force | Out-Null

$mainSources = @(Get-ChildItem (Join-Path $projectRoot "src/main/java") -Filter "*.java" -Recurse | Select-Object -ExpandProperty FullName)
$testSources = @(Get-ChildItem (Join-Path $projectRoot "src/test/java") -Filter "*.java" -Recurse | Select-Object -ExpandProperty FullName)

& javac --release 17 -d $mainOutput $mainSources
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& javac --release 17 -cp $mainOutput -d $testOutput $testSources
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& java -cp "$mainOutput;$testOutput" dev.streamforge.AllTests
exit $LASTEXITCODE
