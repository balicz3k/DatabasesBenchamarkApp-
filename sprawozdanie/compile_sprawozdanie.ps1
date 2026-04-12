# Kompilacja sprawozdanie.tex -> sprawozdanie.pdf (pdflatex dwa razy)
$ErrorActionPreference = "Stop"
$MiKTeXBin = "$env:LOCALAPPDATA\Programs\MiKTeX\miktex\bin\x64"
if (-not (Test-Path "$MiKTeXBin\pdflatex.exe")) {
    Write-Error "Brak pdflatex. Zainstaluj: winget install MiKTeX.MiKTeX"
}
$env:Path = "$MiKTeXBin;$env:Path"
Set-Location $PSScriptRoot

# Pierwsza kompilacja moze pobrac brakujace pakiety (Basic MiKTeX).
for ($i = 1; $i -le 2; $i++) {
    Write-Host "pdflatex przebieg $i / 2..."
    & "$MiKTeXBin\pdflatex.exe" -interaction=nonstopmode -halt-on-error sprawozdanie.tex
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
Write-Host "OK: $PSScriptRoot\sprawozdanie.pdf"
