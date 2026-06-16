$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Drawing

$logPath = Join-Path $PSScriptRoot "tmc_ppt_generation.log"
Set-Content -Path $logPath -Value ("Start: " + (Get-Date).ToString("s"))

$ExportPreview = $false
$CheckpointEverySlides = 2

function Write-GenerationLog {
  param([string]$Message)
  Add-Content -Path $logPath -Value ((Get-Date).ToString("s") + " " + $Message)
}

function Save-Checkpoint {
  param(
    $presentation,
    [string]$outPath,
    [int]$slideNumber
  )
  if ($slideNumber -eq 1) {
    if (-not (Test-Path $outPath)) {
      $presentation.SaveAs($outPath)
      Write-GenerationLog "Initial SaveAs complete"
      return
    }
    $presentation.Save()
    Write-GenerationLog "Checkpoint save after slide 1"
    return
  }

  if (($slideNumber % $CheckpointEverySlides) -eq 0) {
    $presentation.Save()
    Write-GenerationLog ("Checkpoint save after slide " + $slideNumber)
  }
}

function Get-OleColor {
  param([string]$Hex)
  $clean = $Hex.TrimStart("#")
  $color = [System.Drawing.ColorTranslator]::FromHtml("#$clean")
  return [System.Drawing.ColorTranslator]::ToOle($color)
}

function Set-ShapeLine {
  param($shape, [string]$Color = "#1d5fbf", [double]$Weight = 1.2)
  $shape.Line.Visible = -1
  $shape.Line.ForeColor.RGB = Get-OleColor $Color
  $shape.Line.Weight = $Weight
}

function Set-ShapeFill {
  param($shape, [string]$Color)
  $shape.Fill.Visible = -1
  $shape.Fill.Solid()
  $shape.Fill.ForeColor.RGB = Get-OleColor $Color
}

function Add-BoxText {
  param(
    $slide,
    [string]$Text,
    [double]$Left,
    [double]$Top,
    [double]$Width,
    [double]$Height,
    [int]$FontSize = 14,
    [string]$Color = "#1b1b1b",
    [string]$FontName = "Aptos",
    [switch]$Bold,
    [int]$Align = 1,
    [int]$Margin = 6
  )

  $shape = $slide.Shapes.AddTextbox(1, $Left, $Top, $Width, $Height)
  $shape.TextFrame2.TextRange.Text = $Text
  $shape.TextFrame2.MarginLeft = $Margin
  $shape.TextFrame2.MarginRight = $Margin
  $shape.TextFrame2.MarginTop = $Margin
  $shape.TextFrame2.MarginBottom = $Margin
  $shape.TextFrame2.VerticalAnchor = 3
  $shape.Fill.Visible = 0
  $shape.Line.Visible = 0
  $shape.TextFrame2.TextRange.Font.Name = $FontName
  $shape.TextFrame2.TextRange.Font.Size = $FontSize
  $shape.TextFrame2.TextRange.Font.Fill.ForeColor.RGB = Get-OleColor $Color
  $shape.TextFrame2.TextRange.ParagraphFormat.Alignment = $Align
  if ($Bold) {
    $shape.TextFrame2.TextRange.Font.Bold = -1
  }
  return $shape
}

function Add-Card {
  param(
    $slide,
    [double]$Left,
    [double]$Top,
    [double]$Width,
    [double]$Height,
    [string]$Title,
    [string[]]$Body = @(),
    [string]$FillColor = "#ffffff",
    [string]$LineColor = "#1d5fbf",
    [string]$TitleColor = "#0e2349",
    [string]$BodyColor = "#1b1b1b",
    [double]$RadiusWeight = 1.5,
    [switch]$Dark
  )

  $shape = $slide.Shapes.AddShape(5, $Left, $Top, $Width, $Height)
  Set-ShapeFill $shape $FillColor
  Set-ShapeLine $shape $LineColor $RadiusWeight

  $titleShape = Add-BoxText -slide $slide -Text $Title -Left ($Left + 8) -Top ($Top + 4) -Width ($Width - 16) -Height 22 -FontSize 12 -Color $TitleColor -Bold -Align 2
  $bodyText = if ($Body.Count -gt 0) { ($Body | ForEach-Object { "- $_" }) -join "`r" } else { "" }
  $bodyShape = Add-BoxText -slide $slide -Text $bodyText -Left ($Left + 8) -Top ($Top + 28) -Width ($Width - 16) -Height ($Height - 34) -FontSize 8 -Color $BodyColor -Align 1 -Margin 4
  if ($Dark) {
    $titleShape.TextFrame2.TextRange.Font.Fill.ForeColor.RGB = Get-OleColor "#ffffff"
    $bodyShape.TextFrame2.TextRange.Font.Fill.ForeColor.RGB = Get-OleColor "#eef4ff"
  }
  return @($shape, $titleShape, $bodyShape)
}

function Add-SectionBand {
  param(
    $slide,
    [string]$Number,
    [string]$Label,
    [double]$Top,
    [double]$Height
  )

  $band = $slide.Shapes.AddShape(1, 8, $Top, 74, $Height)
  Set-ShapeFill $band "#0c5bbb"
  Set-ShapeLine $band "#0c5bbb" 1

  $circle = $slide.Shapes.AddShape(9, 24, ($Top + 8), 22, 22)
  Set-ShapeFill $circle "#ffffff"
  Set-ShapeLine $circle "#ffffff" 0.8

  $numText = Add-BoxText -slide $slide -Text $Number -Left 24 -Top ($Top + 9) -Width 22 -Height 20 -FontSize 11 -Color "#0e2349" -Bold -Align 2 -Margin 0
  $labelText = Add-BoxText -slide $slide -Text $Label -Left 14 -Top ($Top + 34) -Width 60 -Height ($Height - 38) -FontSize 10 -Color "#ffffff" -Bold -Align 2 -Margin 2
  return @($band, $circle, $numText, $labelText)
}

function Add-Arrow {
  param(
    $slide,
    [double]$X1,
    [double]$Y1,
    [double]$X2,
    [double]$Y2,
    [string]$Color = "#2a6bc7",
    [double]$Weight = 1.6
  )

  $line = $slide.Shapes.AddLine($X1, $Y1, $X2, $Y2)
  $line.Line.ForeColor.RGB = Get-OleColor $Color
  $line.Line.Weight = $Weight
  $line.Line.EndArrowheadStyle = 3
  return $line
}

function Add-Tag {
  param($slide, [string]$Text, [double]$Left, [double]$Top, [double]$Width, [double]$Height, [string]$Fill = "#edf4ff", [string]$Line = "#2a6bc7", [string]$TextColor = "#16376a")
  $shape = $slide.Shapes.AddShape(5, $Left, $Top, $Width, $Height)
  Set-ShapeFill $shape $Fill
  Set-ShapeLine $shape $Line 1
  $shape.Adjustments.Item(1) = 0.1
  $t = Add-BoxText -slide $slide -Text $Text -Left ($Left + 4) -Top ($Top + 2) -Width ($Width - 8) -Height ($Height - 4) -FontSize 8 -Color $TextColor -Bold -Align 2 -Margin 1
  return @($shape, $t)
}

function Add-Image {
  param($slide, [string]$Path, [double]$Left, [double]$Top, [double]$Width, [double]$Height)
  if (Test-Path $Path) {
    return $slide.Shapes.AddPicture((Resolve-Path $Path).Path, 0, -1, $Left, $Top, $Width, $Height)
  }
}

function Add-TitleBlock {
  param($slide, [string]$Title, [string]$Subtitle = "")
  Add-BoxText -slide $slide -Text $Title -Left 90 -Top 10 -Width 820 -Height 26 -FontSize 24 -Color "#08162f" -Bold -Align 2 -Margin 0 | Out-Null
  if ($Subtitle) {
    Add-BoxText -slide $slide -Text $Subtitle -Left 110 -Top 38 -Width 780 -Height 18 -FontSize 10 -Color "#3a4f73" -Align 2 -Margin 0 | Out-Null
  }
}

function Add-RowContainer {
  param($slide, [double]$Top, [double]$Height)
  $shape = $slide.Shapes.AddShape(5, 90, $Top, 850, $Height)
  Set-ShapeFill $shape "#ffffff"
  Set-ShapeLine $shape "#2a6bc7" 1.1
  return $shape
}

$outPath = Join-Path $PSScriptRoot "TMC Process - fancy.pptx"
$previewDir = Join-Path $PSScriptRoot "tmc_ppt_preview"
if (Test-Path $previewDir) {
  Remove-Item $previewDir -Recurse -Force
}
New-Item -ItemType Directory -Path $previewDir | Out-Null

$ppt = New-Object -ComObject PowerPoint.Application
$ppt.Visible = -1
$presentation = $ppt.Presentations.Add()

try {
  Write-GenerationLog "PowerPoint started"
  try {
    $presentation.PageSetup.SlideWidth = 960
    $presentation.PageSetup.SlideHeight = 540
  } catch {}

  $ppLayoutBlank = 12

  # Slide 1 - Cover
  Write-GenerationLog "Building slide 1"
  $slide = $presentation.Slides.Add(1, $ppLayoutBlank)
  $bg = $slide.Shapes.AddShape(1, 0, 0, 960, 540)
  Set-ShapeFill $bg "#f8fbff"
  Set-ShapeLine $bg "#f8fbff" 0
  $accent = $slide.Shapes.AddShape(1, 0, 0, 960, 50)
  Set-ShapeFill $accent "#0c5bbb"
  Set-ShapeLine $accent "#0c5bbb" 0
  Add-BoxText -slide $slide -Text "TMC New System" -Left 44 -Top 92 -Width 410 -Height 34 -FontSize 28 -Color "#08162f" -Bold -Margin 0 | Out-Null
  Add-BoxText -slide $slide -Text "Structure, Workflow, Data Layers and Deployment Architecture" -Left 44 -Top 126 -Width 440 -Height 42 -FontSize 18 -Color "#17386a" -Bold -Margin 0 | Out-Null
  Add-BoxText -slide $slide -Text "Volvo CE thesis prototype aligned to the implemented web application, backend processing, Google Cloud deployment, and PostgreSQL data model." -Left 44 -Top 178 -Width 420 -Height 52 -FontSize 11 -Color "#42587c" -Margin 0 | Out-Null
  Add-Tag -slide $slide -Text "Frontend: React + TypeScript + Vite + Vercel" -Left 44 -Top 252 -Width 212 -Height 24 | Out-Null
  Add-Tag -slide $slide -Text "Backend: FastAPI + Python + Cloud Run" -Left 44 -Top 284 -Width 212 -Height 24 | Out-Null
  Add-Tag -slide $slide -Text "Database: PostgreSQL on Cloud SQL" -Left 44 -Top 316 -Width 212 -Height 24 | Out-Null
  Add-Tag -slide $slide -Text "CI/CD: GitHub Actions + Workload Identity" -Left 44 -Top 348 -Width 228 -Height 24 | Out-Null
  Add-Image -slide $slide -Path "frontend\public\volvo-ce-hero-equipment.jpg" -Left 520 -Top 78 -Width 382 -Height 260 | Out-Null
  $photoBorder = $slide.Shapes.AddShape(5, 510, 68, 402, 280)
  $photoBorder.Fill.Visible = 0
  Set-ShapeLine $photoBorder "#2a6bc7" 1.6
  Add-Image -slide $slide -Path "frontend\public\volvo_construction_equipment_logo.jpg" -Left 590 -Top 368 -Width 236 -Height 68 | Out-Null
  Add-BoxText -slide $slide -Text "Yanxiang Du" -Left 44 -Top 456 -Width 220 -Height 16 -FontSize 10 -Color "#2c4167" -Bold -Margin 0 | Out-Null
  Add-BoxText -slide $slide -Text "Master Thesis | TMC Process Visualizer" -Left 44 -Top 472 -Width 260 -Height 16 -FontSize 9 -Color "#556b8e" -Margin 0 | Out-Null
  Save-Checkpoint -presentation $presentation -outPath $outPath -slideNumber 1

  # Slide 2 - Full architecture
  Write-GenerationLog "Building slide 2"
  $slide = $presentation.Slides.Add(2, $ppLayoutBlank)
  Add-TitleBlock -slide $slide -Title "TMC Web System - Full Functional Architecture" -Subtitle "Fancy high-level view aligned to the implemented prototype, backend workflow, and database footprint."
  Add-SectionBand -slide $slide -Number "1" -Label "User Navigation`r& Pages" -Top 62 -Height 74 | Out-Null
  Add-SectionBand -slide $slide -Number "2" -Label "UI`rActions" -Top 144 -Height 48 | Out-Null
  Add-SectionBand -slide $slide -Number "3" -Label "Backend`rProcessing" -Top 200 -Height 76 | Out-Null
  Add-SectionBand -slide $slide -Number "4" -Label "Pipeline`rStages" -Top 284 -Height 62 | Out-Null
  Add-SectionBand -slide $slide -Number "5" -Label "Data Storage`r(Database)" -Top 354 -Height 132 | Out-Null

  Add-RowContainer -slide $slide -Top 62 -Height 74 | Out-Null
  Add-RowContainer -slide $slide -Top 144 -Height 48 | Out-Null
  Add-RowContainer -slide $slide -Top 200 -Height 76 | Out-Null
  Add-RowContainer -slide $slide -Top 284 -Height 62 | Out-Null
  Add-RowContainer -slide $slide -Top 354 -Height 132 | Out-Null

  Add-Card -slide $slide -Left 102 -Top 74 -Width 88 -Height 48 -Title "Home" -Body @("Landing page", "workflow entry") | Out-Null
  Add-Card -slide $slide -Left 214 -Top 74 -Width 104 -Height 48 -Title "Master Data Entry" -Body @("matrix pages", "planning year") | Out-Null
  Add-Card -slide $slide -Left 344 -Top 74 -Width 142 -Height 48 -Title "Submit Matrix" -Body @("group country", "reporter list", "source matrix", "size class", "brand mapping", "machine line map") | Out-Null
  Add-Card -slide $slide -Left 522 -Top 74 -Width 82 -Height 48 -Title "Upload OTH" -Body @("OTH CSV") | Out-Null
  Add-Card -slide $slide -Left 628 -Top 74 -Width 82 -Height 48 -Title "Upload CRP" -Body @("TMA / SAL") | Out-Null
  Add-Card -slide $slide -Left 734 -Top 68 -Width 188 -Height 60 -Title "View Pipeline" -Body @("P00", "P10", "A10", "SPL", "TMC", "RES", "RPT") | Out-Null
  Add-Arrow -slide $slide -X1 190 -Y1 98 -X2 214 -Y2 98 | Out-Null
  Add-Arrow -slide $slide -X1 318 -Y1 98 -X2 344 -Y2 98 | Out-Null
  Add-Arrow -slide $slide -X1 486 -Y1 98 -X2 522 -Y2 98 | Out-Null
  Add-Arrow -slide $slide -X1 604 -Y1 98 -X2 628 -Y2 98 | Out-Null
  Add-Arrow -slide $slide -X1 710 -Y1 98 -X2 734 -Y2 98 | Out-Null

  $actions = @(
    "Upload", "Show Latest", "Download Latest", "Edit in Page",
    "Table Filters", "Run Report", "Save Snapshot", "Run Tracking", "Status / Error"
  )
  $x = 104
  foreach ($action in $actions) {
    Add-Tag -slide $slide -Text $action -Left $x -Top 155 -Width 86 -Height 24 | Out-Null
    $x += 90
  }

  Add-Card -slide $slide -Left 102 -Top 212 -Width 132 -Height 52 -Title "Upload Service" -Body @("CSV parsing", "header normalization", "row persistence") | Out-Null
  Add-Card -slide $slide -Left 260 -Top 212 -Width 132 -Height 52 -Title "Matrix Resolver" -Body @("country + machine-line matching", "brand / machine mapping") | Out-Null
  Add-Card -slide $slide -Left 418 -Top 212 -Width 132 -Height 52 -Title "Rule Engine" -Body @("reporter flags", "deletion flags", "pri / sec logic", "VCE rules") | Out-Null
  Add-Card -slide $slide -Left 576 -Top 212 -Width 132 -Height 52 -Title "Report Engine" -Body @("P00/P10/A10 generation", "snapshot reuse", "download-ready outputs") | Out-Null
  Add-Card -slide $slide -Left 734 -Top 212 -Width 188 -Height 52 -Title "Run / Versioning Engine" -Body @("new run per upload", "latest successful load", "report history + metadata") | Out-Null
  Add-Arrow -slide $slide -X1 234 -Y1 238 -X2 260 -Y2 238 | Out-Null
  Add-Arrow -slide $slide -X1 392 -Y1 238 -X2 418 -Y2 238 | Out-Null
  Add-Arrow -slide $slide -X1 550 -Y1 238 -X2 576 -Y2 238 | Out-Null
  Add-Arrow -slide $slide -X1 708 -Y1 238 -X2 734 -Y2 238 | Out-Null

  $stageTitles = @("P00","P10","A10","SPL","TMC","RES","RPT")
  $stageBodies = @(
    @("raw prep", "SAL deletion", "OTH cleanup"),
    @("prepared output", "VCE / non-VCE"),
    @("adjustment build"),
    @("machine-line split"),
    @("total market calc", "double-brand cases"),
    @("restatement"),
    @("validation / reporting")
  )
  $sx = 112
  for ($i = 0; $i -lt $stageTitles.Count; $i++) {
    Add-Card -slide $slide -Left $sx -Top 295 -Width 106 -Height 40 -Title $stageTitles[$i] -Body $stageBodies[$i] -LineColor "#3b8c3d" -TitleColor "#143a17" | Out-Null
    if ($i -lt $stageTitles.Count - 1) {
      Add-Arrow -slide $slide -X1 ($sx + 106) -Y1 315 -X2 ($sx + 126) -Y2 315 -Color "#3b8c3d" | Out-Null
    }
    $sx += 116
  }

  Add-Card -slide $slide -Left 102 -Top 366 -Width 150 -Height 108 -Title "A. Upload Metadata" -Body @("upload_runs", "planning_years", "status / timestamps", "matrix type", "run ownership") | Out-Null
  Add-Card -slide $slide -Left 266 -Top 366 -Width 228 -Height 108 -Title "B. Master / Matrix Row Tables" -Body @("group_country_rows", "reporter_list_rows", "source_matrix_rows", "size_class_rows", "brand_mapping_rows", "machine_line_mapping_rows") | Out-Null
  Add-Card -slide $slide -Left 508 -Top 366 -Width 180 -Height 108 -Title "C. Input Data Tables" -Body @("oth_data_rows", "volvo_sale_data_rows", "tma_data_rows", "split_manual_rows") | Out-Null
  Add-Card -slide $slide -Left 702 -Top 366 -Width 220 -Height 108 -Title "D. Report / Snapshot Tables" -Body @("control_report_clean_rows", "crp_tma_report_rows", "crp_sal_report_rows", "p00_report_rows", "excavators_split_case_rows", "report_run_history") | Out-Null
  Add-Tag -slide $slide -Text "All uploads follow the same pattern: create upload_run_id, persist parsed rows, then Show Latest reads the newest successful run." -Left 160 -Top 490 -Width 710 -Height 24 | Out-Null
  Save-Checkpoint -presentation $presentation -outPath $outPath -slideNumber 2

  # Slide 3 - Tech stack
  Write-GenerationLog "Building slide 3"
  $slide = $presentation.Slides.Add(3, $ppLayoutBlank)
  Add-TitleBlock -slide $slide -Title "System Architecture and Development Technologies" -Subtitle "The implemented stack combines a React/Vite frontend, FastAPI API layer, PostgreSQL storage, and automated Google Cloud deployment."
  Add-Image -slide $slide -Path "docs\tmc-new-system-architecture.png" -Left 598 -Top 74 -Width 314 -Height 210 | Out-Null
  $archBorder = $slide.Shapes.AddShape(5, 590, 66, 330, 226)
  $archBorder.Fill.Visible = 0
  Set-ShapeLine $archBorder "#2a6bc7" 1.2

  Add-Card -slide $slide -Left 58 -Top 86 -Width 236 -Height 104 -Title "Frontend Layer" -Body @("React + TypeScript", "Vite build pipeline", "React Router navigation", "Filterable tables + interactive report pages", "Vercel hosting for user-facing UI") | Out-Null
  Add-Card -slide $slide -Left 320 -Top 86 -Width 236 -Height 104 -Title "Backend Layer" -Body @("FastAPI routers", "CSV upload services", "report execution endpoints", "snapshot saving + reuse", "CORS + environment-driven deployment") | Out-Null
  Add-Card -slide $slide -Left 58 -Top 214 -Width 236 -Height 104 -Title "Database Layer" -Body @("PostgreSQL on Cloud SQL", "normalized upload metadata", "matrix row tables", "report row tables", "run history and planning year support") | Out-Null
  Add-Card -slide $slide -Left 320 -Top 214 -Width 236 -Height 104 -Title "Deployment Layer" -Body @("GitHub Actions workflow", "Workload Identity Federation", "Artifact Registry images", "Cloud Run service deployment", "frontend-backend split across Vercel + GCP") | Out-Null
  Add-Arrow -slide $slide -X1 294 -Y1 138 -X2 320 -Y2 138 | Out-Null
  Add-Arrow -slide $slide -X1 176 -Y1 190 -X2 176 -Y2 214 | Out-Null
  Add-Arrow -slide $slide -X1 438 -Y1 190 -X2 438 -Y2 214 | Out-Null

  Add-Card -slide $slide -Left 74 -Top 346 -Width 838 -Height 118 -Title "Project-Aligned Implementation Notes" -Body @(
    "Core pages already implemented: Matrix Submission, OTH Upload, CRP Upload, Pipeline Viewer, Total Market Calculation, Restatement, and TMC Validation Report.",
    "Backend logic is concentrated in uploads.py plus shared DB helpers, with the same API also supporting latest-run retrieval, editing, and download workflows.",
    "This architecture replaces hidden SAP-heavy steps with a browser-accessible prototype that exposes intermediate layers for thesis demonstration and business review."
  ) | Out-Null
  Save-Checkpoint -presentation $presentation -outPath $outPath -slideNumber 3

  # Slide 4 - Core tables
  Write-GenerationLog "Building slide 4"
  $slide = $presentation.Slides.Add(4, $ppLayoutBlank)
  Add-TitleBlock -slide $slide -Title "Core Tables and Data Persistence Model" -Subtitle "The data model separates upload metadata, matrix configuration rows, raw input tables, and calculated report outputs."
  Add-Card -slide $slide -Left 48 -Top 72 -Width 190 -Height 164 -Title "Upload Metadata" -Body @("upload_runs", "planning_years", "status", "row_count", "planning_year", "file_name", "created_by") | Out-Null
  Add-Card -slide $slide -Left 256 -Top 72 -Width 190 -Height 164 -Title "Matrix Configuration Rows" -Body @("group_country_rows", "reporter_list_rows", "source_matrix_rows", "size_class_rows", "brand_mapping_rows", "machine_line_mapping_rows") | Out-Null
  Add-Card -slide $slide -Left 464 -Top 72 -Width 190 -Height 164 -Title "Input / Source Data Rows" -Body @("oth_data_rows", "volvo_sale_data_rows", "tma_data_rows", "split_manual_rows") | Out-Null
  Add-Card -slide $slide -Left 672 -Top 72 -Width 240 -Height 164 -Title "Report Output Tables" -Body @("control_report_clean_rows", "crp_tma_report_rows", "crp_sal_report_rows", "p00_report_rows", "excavators_split_case_rows") | Out-Null
  Add-Arrow -slide $slide -X1 238 -Y1 154 -X2 256 -Y2 154 | Out-Null
  Add-Arrow -slide $slide -X1 446 -Y1 154 -X2 464 -Y2 154 | Out-Null
  Add-Arrow -slide $slide -X1 654 -Y1 154 -X2 672 -Y2 154 | Out-Null

  Add-Card -slide $slide -Left 58 -Top 262 -Width 854 -Height 86 -Title "Run and Snapshot Tracking" -Body @("report_run_history records report type, step code, start / finish times, status, and messages.", "Snapshot-saving endpoints keep editable output states so business users can continue from reviewed data instead of recomputing everything.", "The same pattern is reused for P00, total-market snapshots, and excavators split case outputs.") | Out-Null

  Add-Image -slide $slide -Path "docs\tmc-full-architecture-data-storage-clear.png" -Left 120 -Top 364 -Width 730 -Height 128 | Out-Null
  $imgLabel = Add-Tag -slide $slide -Text "Reference architecture visual already produced inside the project docs" -Left 280 -Top 500 -Width 420 -Height 22
  Save-Checkpoint -presentation $presentation -outPath $outPath -slideNumber 4

  # Slide 5 - Workflow details
  Write-GenerationLog "Building slide 5"
  $slide = $presentation.Slides.Add(5, $ppLayoutBlank)
  Add-TitleBlock -slide $slide -Title "Process Flow from Upload to TMC, Restatement and Validation" -Subtitle "This flow matches the real pages and report endpoints rather than a generic conceptual pipeline."
  Add-Card -slide $slide -Left 62 -Top 82 -Width 122 -Height 60 -Title "Master Data" -Body @("source matrix", "reporters", "country groups", "brand map") | Out-Null
  Add-Card -slide $slide -Left 196 -Top 82 -Width 110 -Height 60 -Title "P00" -Body @("raw prep", "SAL / OTH cleanup") | Out-Null
  Add-Card -slide $slide -Left 318 -Top 82 -Width 110 -Height 60 -Title "P10" -Body @("prepared TMA", "VCE / non-VCE") | Out-Null
  Add-Card -slide $slide -Left 440 -Top 82 -Width 110 -Height 60 -Title "A10" -Body @("adjustment-ready rows") | Out-Null
  Add-Card -slide $slide -Left 562 -Top 82 -Width 110 -Height 60 -Title "SPL" -Body @("machine-line split") | Out-Null
  Add-Card -slide $slide -Left 684 -Top 82 -Width 110 -Height 60 -Title "TMC" -Body @("double-brand logic", "market aggregation") | Out-Null
  Add-Card -slide $slide -Left 806 -Top 82 -Width 110 -Height 60 -Title "RES / VAL" -Body @("restatement", "validation review") | Out-Null
  foreach ($start in @(184,306,428,550,672,794)) {
    Add-Arrow -slide $slide -X1 $start -Y1 112 -X2 ($start + 12) -Y2 112 -Color "#2a6bc7" | Out-Null
  }

  Add-Image -slide $slide -Path "tmc_full_architecture_up_to_machine_line_split_with_storage_note.png" -Left 54 -Top 162 -Width 416 -Height 256 | Out-Null
  $imgBorder = $slide.Shapes.AddShape(5, 48, 156, 428, 268)
  $imgBorder.Fill.Visible = 0
  Set-ShapeLine $imgBorder "#2a6bc7" 1.1

  Add-Card -slide $slide -Left 506 -Top 170 -Width 392 -Height 116 -Title "Frontend to Backend Interaction Pattern" -Body @(
    "Pages call upload / report endpoints through the shared API layer in frontend/src/api/uploads.ts.",
    "Users can run a report, read the latest successful result, edit values in-page, and save a snapshot back to PostgreSQL.",
    "This makes the prototype useful both as a visualizer and as an operational review workspace."
  ) | Out-Null
  Add-Card -slide $slide -Left 506 -Top 304 -Width 392 -Height 120 -Title "Why this matters for the thesis" -Body @(
    "The prototype exposes hidden intermediate calculation layers that were previously difficult to inspect in legacy workflows.",
    "Business logic becomes traceable at row level through filters, downloads, run metadata, and validation pages.",
    "That traceability is the bridge between SAP-heavy legacy processing and a modern browser-based decision interface."
  ) | Out-Null
  Save-Checkpoint -presentation $presentation -outPath $outPath -slideNumber 5

  # Slide 6 - Deployment
  Write-GenerationLog "Building slide 6"
  $slide = $presentation.Slides.Add(6, $ppLayoutBlank)
  Add-TitleBlock -slide $slide -Title "Deployment, Hosting, and Automatic Release Flow" -Subtitle "Frontend and backend are intentionally separated: Vercel for UI delivery, Google Cloud for API execution and database hosting."
  Add-Card -slide $slide -Left 52 -Top 86 -Width 122 -Height 56 -Title "GitHub Repo" -Body @("main branch", "backend workflow") | Out-Null
  Add-Card -slide $slide -Left 194 -Top 86 -Width 136 -Height 56 -Title "GitHub Actions" -Body @("deploy-backend.yml", "build + push + deploy") | Out-Null
  Add-Card -slide $slide -Left 350 -Top 86 -Width 154 -Height 56 -Title "Workload Identity" -Body @("federated auth", "repo trust mapping") | Out-Null
  Add-Card -slide $slide -Left 524 -Top 86 -Width 144 -Height 56 -Title "GCP Service Account" -Body @("cloudrun deployer", "Artifact Registry / Cloud Run roles") | Out-Null
  Add-Card -slide $slide -Left 688 -Top 86 -Width 118 -Height 56 -Title "Artifact Registry" -Body @("container image") | Out-Null
  Add-Card -slide $slide -Left 826 -Top 86 -Width 90 -Height 56 -Title "Cloud Run" -Body @("FastAPI service") | Out-Null
  foreach ($start in @(174,330,504,668,806)) {
    Add-Arrow -slide $slide -X1 $start -Y1 114 -X2 ($start + 18) -Y2 114 -Color "#2a6bc7" | Out-Null
  }
  Add-Card -slide $slide -Left 194 -Top 170 -Width 312 -Height 96 -Title "GitHub Variables Used by the Workflow" -Body @("GCP_PROJECT_ID", "GCP_REGION", "GCP_SERVICE", "GCP_SERVICE_ACCOUNT", "GCP_WORKLOAD_IDENTITY_PROVIDER") | Out-Null
  Add-Card -slide $slide -Left 530 -Top 170 -Width 386 -Height 96 -Title "Cloud Run Runtime Configuration" -Body @("CORS_ALLOW_ORIGINS", "DATABASE_URL using /cloudsql/... socket", "Cloud SQL connection binding", "automatic rollout of latest image revision") | Out-Null
  Add-Card -slide $slide -Left 52 -Top 286 -Width 258 -Height 114 -Title "Frontend Hosting" -Body @("Vercel hosts the React/Vite frontend.", "Users load the UI from Vercel, then the browser calls the Cloud Run API.", "CORS must therefore explicitly allow the Vercel domains.") | Out-Null
  Add-Card -slide $slide -Left 330 -Top 286 -Width 258 -Height 114 -Title "Backend Hosting" -Body @("Cloud Run scales the FastAPI container.", "The backend is stateless and listens on port 8080.", "Auto-deploy creates new revisions, visible in Cloud Run > Revisions.") | Out-Null
  Add-Card -slide $slide -Left 608 -Top 286 -Width 308 -Height 114 -Title "Database Hosting" -Body @("Cloud SQL hosts PostgreSQL.", "Cloud Run reaches it through the bound Cloud SQL instance.", "The backend persists upload data, report rows, snapshots, and run history here.") | Out-Null
  Add-Image -slide $slide -Path "docs\tmc-full-architecture-up-to-spl.png" -Left 168 -Top 416 -Width 624 -Height 78 | Out-Null
  Save-Checkpoint -presentation $presentation -outPath $outPath -slideNumber 6

  # Slide 7 - Scope and open points
  Write-GenerationLog "Building slide 7"
  $slide = $presentation.Slides.Add(7, $ppLayoutBlank)
  Add-TitleBlock -slide $slide -Title "Current Scope, Strengths, and Open Operational Points" -Subtitle "The prototype is already functional, but still intentionally thesis-scoped rather than production-hardened."
  Add-Card -slide $slide -Left 56 -Top 82 -Width 262 -Height 150 -Title "What is already strong" -Body @("Clear separation between frontend, backend, and database.", "Real pipeline layers are visible instead of hidden.", "Automatic backend deployment is working.", "Data is persisted and can be reloaded by latest successful run.") | Out-Null
  Add-Card -slide $slide -Left 348 -Top 82 -Width 262 -Height 150 -Title "What is thesis-prototype scope" -Body @("Not every legacy business rule is yet fully covered.", "Some workflows still depend on careful data preparation and matrix quality.", "UI focuses on transparency and experimentation rather than full enterprise hardening.") | Out-Null
  Add-Card -slide $slide -Left 640 -Top 82 -Width 262 -Height 150 -Title "Operational open points" -Body @("Cloud SQL cost is usually the less elastic part compared with Cloud Run.", "Security hardening, secrets handling, and backup strategy would need more work for production.", "Monitoring and alerting can be expanded beyond the current prototype level.") | Out-Null
  Add-Image -slide $slide -Path "restatement_double_brand_check_simplified.png" -Left 72 -Top 252 -Width 386 -Height 188 | Out-Null
  $dbBorder = $slide.Shapes.AddShape(5, 66, 246, 398, 200)
  $dbBorder.Fill.Visible = 0
  Set-ShapeLine $dbBorder "#2a6bc7" 1.1
  Add-Card -slide $slide -Left 500 -Top 256 -Width 390 -Height 180 -Title "Interpretation for the thesis story" -Body @(
    "The project demonstrates that a complex TMC process can be decomposed into understandable, reviewable modules without losing connection to the underlying data.",
    "That makes the system useful both for process explanation and for reducing dependence on opaque legacy execution paths.",
    "In presentation terms, the key message is not just that the web app exists, but that the architecture supports transparency, traceability, and staged review."
  ) | Out-Null
  Save-Checkpoint -presentation $presentation -outPath $outPath -slideNumber 7

  # Slide 8 - Closing
  Write-GenerationLog "Building slide 8"
  $slide = $presentation.Slides.Add(8, $ppLayoutBlank)
  $bg = $slide.Shapes.AddShape(1, 0, 0, 960, 540)
  Set-ShapeFill $bg "#f8fbff"
  Set-ShapeLine $bg "#f8fbff" 0
  Add-Image -slide $slide -Path "frontend\public\volvo_construction_equipment_logo.jpg" -Left 332 -Top 72 -Width 296 -Height 86 | Out-Null
  Add-BoxText -slide $slide -Text "Thank You" -Left 250 -Top 210 -Width 460 -Height 44 -FontSize 30 -Color "#08162f" -Bold -Align 2 -Margin 0 | Out-Null
  Add-BoxText -slide $slide -Text "Questions, feedback, and discussion on the TMC Process Visualizer are welcome." -Left 190 -Top 262 -Width 580 -Height 22 -FontSize 13 -Color "#35507b" -Align 2 -Margin 0 | Out-Null
  Add-Tag -slide $slide -Text "Frontend on Vercel" -Left 250 -Top 330 -Width 136 -Height 24 | Out-Null
  Add-Tag -slide $slide -Text "Backend on Cloud Run" -Left 404 -Top 330 -Width 154 -Height 24 | Out-Null
  Add-Tag -slide $slide -Text "PostgreSQL on Cloud SQL" -Left 576 -Top 330 -Width 164 -Height 24 | Out-Null
  Save-Checkpoint -presentation $presentation -outPath $outPath -slideNumber 8

  $presentation.Save()
  Write-GenerationLog "Final save complete"

  if ($ExportPreview) {
    foreach ($s in $presentation.Slides) {
      $exportPath = Join-Path $previewDir ("slide-{0}.png" -f $s.SlideIndex)
      $s.Export($exportPath, "PNG", 1600, 900)
      Write-GenerationLog ("Exported slide " + $s.SlideIndex)
    }
  }
}
finally {
  Write-GenerationLog "Closing PowerPoint"
  $presentation.Close()
  $ppt.Quit()
  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($presentation) | Out-Null
  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($ppt) | Out-Null
  [GC]::Collect()
  [GC]::WaitForPendingFinalizers()
}
