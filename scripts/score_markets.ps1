# score_markets.ps1 — Apply TBH scoring model to Redfin metro data
# Outputs: data/scored_markets.csv, data/top25.csv

$ErrorActionPreference = 'Stop'
$dataDir = Join-Path $PSScriptRoot '..\data'
$tsvFile = Join-Path $dataDir 'redfin_metro_raw.tsv'

Write-Host "Reading Redfin metro TSV..."

# Find the latest period end date for SFR
$latestDate = $null
$header = $null
$rows = @()

$reader = [System.IO.StreamReader]::new($tsvFile)
$headerLine = $reader.ReadLine()
$header = $headerLine -replace '"', '' -split "`t"

# Find column indices
$colMap = @{}
for ($i = 0; $i -lt $header.Count; $i++) { $colMap[$header[$i]] = $i }

$periodEndIdx = $colMap['PERIOD_END']
$regionIdx = $colMap['REGION']
$stateIdx = $colMap['STATE_CODE']
$propTypeIdx = $colMap['PROPERTY_TYPE']
$yoyIdx = $colMap['MEDIAN_SALE_PRICE_YOY']
$domIdx = $colMap['MEDIAN_DOM']
$pendingIdx = $colMap['PENDING_SALES']
$inventoryIdx = $colMap['INVENTORY']
$medPriceIdx = $colMap['MEDIAN_SALE_PRICE']
$homesSoldIdx = $colMap['HOMES_SOLD']

Write-Host "Scanning for latest period..."

# First pass: find latest date for SFR metros
$maxDate = ''
$lineCount = 0
while ($null -ne ($line = $reader.ReadLine())) {
    $lineCount++
    $fields = $line -replace '"', '' -split "`t"
    if ($fields[$propTypeIdx] -eq 'Single Family Residential' -and $fields[$periodEndIdx] -gt $maxDate) {
        $maxDate = $fields[$periodEndIdx]
    }
}
$reader.Close()
Write-Host "Latest period: $maxDate ($lineCount total rows scanned)"

# Second pass: extract rows for latest period SFR
Write-Host "Extracting latest SFR metro data..."
$reader = [System.IO.StreamReader]::new($tsvFile)
$null = $reader.ReadLine() # skip header

$markets = @()
while ($null -ne ($line = $reader.ReadLine())) {
    $fields = $line -replace '"', '' -split "`t"
    if ($fields[$propTypeIdx] -eq 'Single Family Residential' -and $fields[$periodEndIdx] -eq $maxDate) {
        $yoyRaw = $fields[$yoyIdx]
        $domRaw = $fields[$domIdx]
        $pendingRaw = $fields[$pendingIdx]
        $invRaw = $fields[$inventoryIdx]
        $priceRaw = $fields[$medPriceIdx]
        $salesRaw = $fields[$homesSoldIdx]

        # Skip if any key metric is NA
        if ($yoyRaw -eq 'NA' -or $domRaw -eq 'NA' -or $pendingRaw -eq 'NA' -or $invRaw -eq 'NA') { continue }
        if ($invRaw -eq '0' -or $invRaw -eq '') { continue }

        $yoy = [double]$yoyRaw * 100  # Convert from decimal to percentage
        $dom = [int]$domRaw
        $pending = [double]$pendingRaw
        $inv = [double]$invRaw
        $ratio = [math]::Round($pending / $inv, 3)
        $price = if ($priceRaw -ne 'NA') { [int]$priceRaw } else { 0 }
        $sales = if ($salesRaw -ne 'NA') { [int]$salesRaw } else { 0 }

        $markets += [PSCustomObject]@{
            Region = $fields[$regionIdx] -replace ' metro area', ''
            State = $fields[$stateIdx]
            YOY = [math]::Round($yoy, 2)
            DOM = $dom
            Ratio = $ratio
            MedPrice = $price
            Sales = $sales
        }
    }
}
$reader.Close()
Write-Host "Found $($markets.Count) SFR metros for period $maxDate"

# Apply qualifying thresholds
$qualified = $markets | Where-Object { $_.YOY -ge 5 -and $_.DOM -le 50 -and $_.Ratio -ge 0.5 }
Write-Host "Qualified (YOY>=5, DOM<=50, Ratio>=0.5): $($qualified.Count)"

# Score function
function Get-CompositeScore {
    param($yoy, $dom, $ratio)
    
    # YOY points (40% weight)
    $yoyPts = 0
    if ($yoy -ge 20) { $yoyPts = 40 }
    elseif ($yoy -ge 15) { $yoyPts = 34 }
    elseif ($yoy -ge 8) { $yoyPts = 28 }
    elseif ($yoy -ge 5) { $yoyPts = 20 }
    
    # DOM points (30% weight)
    $domPts = 0
    if ($dom -lt 20) { $domPts = 30 }
    elseif ($dom -le 35) { $domPts = 22 }
    elseif ($dom -le 50) { $domPts = 15 }
    
    # Ratio points (30% weight)
    $ratioPts = 0
    if ($ratio -ge 1.0) { $ratioPts = 30 }
    elseif ($ratio -ge 0.75) { $ratioPts = 22 }
    elseif ($ratio -ge 0.5) { $ratioPts = 15 }
    
    return ($yoyPts + $domPts + $ratioPts)
}

# Score all qualified markets
$scored = $qualified | ForEach-Object {
    $score = Get-CompositeScore $_.YOY $_.DOM $_.Ratio
    $_ | Add-Member -NotePropertyName 'Score' -NotePropertyValue $score -PassThru
} | Sort-Object Score -Descending

# Output all scored
$scoredFile = Join-Path $dataDir 'scored_markets.csv'
$scored | Select-Object Region, State, Score, YOY, DOM, Ratio, MedPrice, Sales |
    Export-Csv -Path $scoredFile -NoTypeInformation
Write-Host "Wrote $($scored.Count) scored markets to scored_markets.csv"

# Top 25
$top25 = $scored | Select-Object -First 25
$top25File = Join-Path $dataDir 'top25.csv'
$top25 | Select-Object Region, State, Score, YOY, DOM, Ratio, MedPrice, Sales |
    Export-Csv -Path $top25File -NoTypeInformation
Write-Host "`n=== TOP 25 MARKETS ==="
$rank = 0
$top25 | ForEach-Object {
    $rank++
    $msg = "#{0} {1}, {2} - Score: {3} | YOY: {4}% | DOM: {5} | Ratio: {6} | Price: ${7} | Sales: {8}" -f $rank, $_.Region, $_.State, $_.Score, $_.YOY, $_.DOM, $_.Ratio, $_.MedPrice, $_.Sales
    Write-Host $msg
}
Write-Host "Done. Data period: $maxDate"
