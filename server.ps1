param(
  [int]$Port = $(if ([string]::IsNullOrWhiteSpace($env:PORT)) { 8787 } else { [int]$env:PORT }),
  [string]$DataPath = (Join-Path $PSScriptRoot "data\catalog.json"),
  [string]$UploadRoot = (Join-Path $PSScriptRoot "uploads"),
  [string]$LogPath = (Join-Path $PSScriptRoot "logs\server.log"),
  [string]$ApiToken = $env:KKM_API_TOKEN,
  [int]$MaxFilesPerBatch = 10,
  [int]$MaxRequestBytes = 26214400,
  [int]$MaxFileBytes = 8388608,
  [string]$HiDriveBaseUrl = $(if ([string]::IsNullOrWhiteSpace($env:HIDRIVE_BASE_URL)) { "https://webdav.hidrive.strato.com" } else { $env:HIDRIVE_BASE_URL }),
  [string]$HiDriveUsername = $env:HIDRIVE_USERNAME,
  [string]$HiDrivePassword = $env:HIDRIVE_PASSWORD,
  [string]$HiDriveRoot = $(if ([string]::IsNullOrWhiteSpace($env:HIDRIVE_ROOT)) { "KKM Capture" } else { $env:HIDRIVE_ROOT })
)

$ErrorActionPreference = "Stop"

function Ensure-ParentDirectory {
  param([string]$Path)

  $parent = Split-Path -Path $Path -Parent
  if (-not [string]::IsNullOrWhiteSpace($parent) -and
      -not (Test-Path -LiteralPath $parent)) {
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
  }
}

function Write-LogEntry {
  param(
    [string]$Level,
    [string]$Message,
    [string]$RequestId = "",
    [string]$Method = "",
    [string]$Path = "",
    [int]$StatusCode = 0,
    [hashtable]$Data = @{}
  )

  Ensure-ParentDirectory -Path $LogPath

  $entry = [ordered]@{
    timestamp = (Get-Date).ToUniversalTime().ToString("o")
    level = $Level
    message = $Message
    requestId = $RequestId
    method = $Method
    path = $Path
    statusCode = $StatusCode
    data = $Data
  }

  $json = $entry | ConvertTo-Json -Depth 12 -Compress
  Add-Content -LiteralPath $LogPath -Value $json -Encoding UTF8
}

function Read-Catalog {
  if (-not (Test-Path -LiteralPath $DataPath)) {
    return [pscustomobject]@{
      projects = @()
      hdds = @()
    }
  }

  $raw = Get-Content -LiteralPath $DataPath -Raw -Encoding UTF8
  if ([string]::IsNullOrWhiteSpace($raw)) {
    return [pscustomobject]@{
      projects = @()
      hdds = @()
    }
  }

  return $raw | ConvertFrom-Json
}

function Ensure-UploadRoot {
  if (-not (Test-Path -LiteralPath $UploadRoot)) {
    New-Item -ItemType Directory -Path $UploadRoot -Force | Out-Null
  }
}

function Ensure-Directory {
  param([string]$Path)

  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
  }

  return $Path
}

function Test-HiDriveConfigured {
  return (-not [string]::IsNullOrWhiteSpace($HiDriveUsername)) -and
    (-not [string]::IsNullOrWhiteSpace($HiDrivePassword))
}

function Get-StorageMode {
  if (Test-HiDriveConfigured) {
    return "hidrive-webdav"
  }

  return "local"
}

function Get-HiDriveRootSegments {
  if ([string]::IsNullOrWhiteSpace($HiDriveRoot)) {
    return @()
  }

  $trimmed = $HiDriveRoot.Trim([char[]]"/\")
  if ([string]::IsNullOrWhiteSpace($trimmed)) {
    return @()
  }

  return @($trimmed.Split(
    [char[]]@('/', [char]'\'),
    [System.StringSplitOptions]::RemoveEmptyEntries
  ))
}

function Join-StoragePath {
  param([string[]]$Segments = @())

  $parts = @($Segments | Where-Object {
    -not [string]::IsNullOrWhiteSpace([string]$_)
  })
  if ($parts.Count -eq 0) {
    return ""
  }

  return ($parts -join "/")
}

function Get-StorageTargetDescription {
  if ((Get-StorageMode) -eq "hidrive-webdav") {
    $target = Join-StoragePath -Segments (Get-HiDriveRootSegments)
    if ([string]::IsNullOrWhiteSpace($target)) {
      return "/"
    }

    return $target
  }

  return $UploadRoot
}

function New-WebDavUri {
  param([string[]]$PathSegments = @())

  $base = [string]$HiDriveBaseUrl
  if ([string]::IsNullOrWhiteSpace($base)) {
    throw "HiDriveBaseUrl ist leer."
  }

  $base = $base.Trim().TrimEnd('/')
  $encodedSegments = @()
  foreach ($segment in $PathSegments) {
    if ([string]::IsNullOrWhiteSpace([string]$segment)) {
      continue
    }

    $encodedSegments += [System.Uri]::EscapeDataString([string]$segment)
  }

  if ($encodedSegments.Count -eq 0) {
    return [System.Uri]::new("$base/")
  }

  return [System.Uri]::new($base + "/" + ($encodedSegments -join "/"))
}

function Get-WebDavAuthorizationHeader {
  $rawValue = "{0}:{1}" -f $HiDriveUsername, $HiDrivePassword
  $authBytes = [System.Text.Encoding]::UTF8.GetBytes($rawValue)
  return "Basic " + [Convert]::ToBase64String($authBytes)
}

function Invoke-WebDavRequest {
  param(
    [string]$Method,
    [string[]]$PathSegments = @(),
    [hashtable]$Headers = @{},
    [byte[]]$BodyBytes,
    [string]$BodyText = "",
    [string]$ContentType = ""
  )

  $client = $null
  $message = $null
  $response = $null

  try {
    $uri = New-WebDavUri -PathSegments $PathSegments
    $message = [System.Net.Http.HttpRequestMessage]::new(
      [System.Net.Http.HttpMethod]::new($Method),
      $uri
    )
    [void]$message.Headers.TryAddWithoutValidation(
      "Authorization",
      (Get-WebDavAuthorizationHeader)
    )

    foreach ($entry in $Headers.GetEnumerator()) {
      [void]$message.Headers.TryAddWithoutValidation(
        [string]$entry.Key,
        [string]$entry.Value
      )
    }

    $hasBody = $PSBoundParameters.ContainsKey("BodyBytes")
    if (-not $hasBody -and $PSBoundParameters.ContainsKey("BodyText")) {
      $BodyBytes = [System.Text.Encoding]::UTF8.GetBytes([string]$BodyText)
      $hasBody = $true
    }

    if ($hasBody) {
      $content = [System.Net.Http.ByteArrayContent]::new([byte[]]$BodyBytes)
      if (-not [string]::IsNullOrWhiteSpace($ContentType)) {
        $content.Headers.ContentType =
          [System.Net.Http.Headers.MediaTypeHeaderValue]::Parse($ContentType)
      }
      $message.Content = $content
    }

    $client = [System.Net.Http.HttpClient]::new()
    $client.Timeout = [TimeSpan]::FromSeconds(90)

    $response = $client.SendAsync($message).GetAwaiter().GetResult()
    $responseText = ""
    if ($null -ne $response.Content) {
      $responseText = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    }

    return [pscustomobject]@{
      StatusCode = [int]$response.StatusCode
      ReasonPhrase = [string]$response.ReasonPhrase
      Body = $responseText
      Uri = $uri.AbsoluteUri
    }
  } finally {
    if ($null -ne $response) {
      $response.Dispose()
    }
    if ($null -ne $message) {
      $message.Dispose()
    }
    if ($null -ne $client) {
      $client.Dispose()
    }
  }
}

function Get-WebDavPropfindBody {
  return '<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:displayname /><d:resourcetype /></d:prop></d:propfind>'
}

function Ensure-HiDriveDirectory {
  param([string[]]$PathSegments)

  $currentSegments = @()
  foreach ($segment in $PathSegments) {
    if ([string]::IsNullOrWhiteSpace([string]$segment)) {
      continue
    }

    $currentSegments += [string]$segment
    $response = Invoke-WebDavRequest -Method "MKCOL" -PathSegments $currentSegments
    if ($response.StatusCode -notin @(200, 201, 204, 301, 302, 405)) {
      throw (
        "WebDAV-Verzeichnis '{0}' konnte nicht angelegt werden (HTTP {1})." -f
        (Join-StoragePath -Segments $currentSegments),
        $response.StatusCode
      )
    }
  }

  return (Join-StoragePath -Segments $PathSegments)
}

function Get-WebDavNameFromHref {
  param([string]$Href)

  if ([string]::IsNullOrWhiteSpace($Href)) {
    return ""
  }

  $path = $Href
  try {
    if ($Href -match '^https?://') {
      $path = ([System.Uri]::new($Href)).AbsolutePath
    }
  } catch {
    $path = $Href
  }

  $trimmed = [string]$path
  while ($trimmed.EndsWith('/')) {
    $trimmed = $trimmed.Substring(0, $trimmed.Length - 1)
  }

  if ([string]::IsNullOrWhiteSpace($trimmed)) {
    return ""
  }

  $name = [System.IO.Path]::GetFileName($trimmed)
  if ([string]::IsNullOrWhiteSpace($name)) {
    return ""
  }

  return [System.Uri]::UnescapeDataString($name)
}

function Get-WebDavFiles {
  param([string[]]$PathSegments)

  $response = Invoke-WebDavRequest `
    -Method "PROPFIND" `
    -PathSegments $PathSegments `
    -Headers @{ Depth = "1" } `
    -BodyText (Get-WebDavPropfindBody) `
    -ContentType "application/xml; charset=utf-8"

  if ($response.StatusCode -eq 404) {
    return @()
  }

  if ($response.StatusCode -notin @(200, 207)) {
    throw "WebDAV-Dateiliste konnte nicht geladen werden (HTTP $($response.StatusCode))."
  }

  if ([string]::IsNullOrWhiteSpace($response.Body)) {
    return @()
  }

  try {
    [xml]$xml = $response.Body
  } catch {
    throw "WebDAV-Dateiliste ist kein gueltiges XML."
  }

  $namespaceManager = [System.Xml.XmlNamespaceManager]::new($xml.NameTable)
  $namespaceManager.AddNamespace("d", "DAV:")
  $responses = $xml.SelectNodes("//d:response", $namespaceManager)
  $files = @()

  foreach ($node in $responses) {
    $collectionNode = $node.SelectSingleNode(
      ".//d:resourcetype/d:collection",
      $namespaceManager
    )
    if ($null -ne $collectionNode) {
      continue
    }

    $hrefNode = $node.SelectSingleNode("d:href", $namespaceManager)
    if ($null -eq $hrefNode) {
      continue
    }

    $name = Get-WebDavNameFromHref -Href ([string]$hrefNode.InnerText)
    if ([string]::IsNullOrWhiteSpace($name)) {
      continue
    }

    $files += [pscustomobject]@{
      Name = $name
    }
  }

  return $files
}
function Get-PathSegments {
  param([System.Uri]$Url)

  $path = $Url.AbsolutePath.Trim([char[]]"/")
  if ([string]::IsNullOrWhiteSpace($path)) {
    return @()
  }

  return @($path -split "/" | ForEach-Object {
    [System.Uri]::UnescapeDataString($_)
  })
}

function New-ApiResponse {
  param(
    [int]$StatusCode,
    [object]$Payload
  )

  return [pscustomobject]@{
    StatusCode = $StatusCode
    Payload = $Payload
  }
}

function New-ApiErrorResponse {
  param(
    [int]$StatusCode,
    [string]$Code,
    [string]$Message,
    [string]$RequestId = "",
    [object]$Details = $null
  )

  $payload = [ordered]@{
    status = "error"
    code = $Code
    message = $Message
    error = $Message
  }

  if (-not [string]::IsNullOrWhiteSpace($RequestId)) {
    $payload.requestId = $RequestId
  }

  if ($null -ne $Details) {
    $payload.details = $Details
  }

  return New-ApiResponse -StatusCode $StatusCode -Payload $payload
}

function New-RequestId {
  return [guid]::NewGuid().ToString("N")
}

function Test-ApiAuthorization {
  param([hashtable]$Headers)

  if ([string]::IsNullOrWhiteSpace($ApiToken)) {
    return $true
  }

  $authorization = [string]$Headers["Authorization"]
  if (-not [string]::IsNullOrWhiteSpace($authorization) -and
      $authorization.StartsWith("Bearer ", [System.StringComparison]::OrdinalIgnoreCase)) {
    $providedToken = $authorization.Substring(7).Trim()
    return $providedToken -eq $ApiToken
  }

  $headerToken = [string]$Headers["X-KKM-Token"]
  if (-not [string]::IsNullOrWhiteSpace($headerToken)) {
    return $headerToken.Trim() -eq $ApiToken
  }

  return $false
}

function Test-AllowedImageFile {
  param([pscustomobject]$File)

  $contentType = [string]$File.Headers["Content-Type"]
  if ([string]::IsNullOrWhiteSpace($contentType)) {
    return $false
  }

  return $contentType.ToLowerInvariant() -in @(
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/webp",
    "image/heic"
  )
}

function Get-MultipartBoundary {
  param([string]$ContentType)

  $match = [regex]::Match($ContentType, 'boundary="?([^";]+)"?')
  if (-not $match.Success) {
    return $null
  }

  return $match.Groups[1].Value
}

function ConvertFrom-MultipartFormData {
  param(
    [byte[]]$Body,
    [string]$Boundary
  )

  $latin1 = [System.Text.Encoding]::GetEncoding("ISO-8859-1")
  $rawText = $latin1.GetString($Body)
  $marker = "--$Boundary"
  $sections = $rawText -split [regex]::Escape($marker)

  $fields = @{}
  $files = @()

  foreach ($section in $sections) {
    if ([string]::IsNullOrWhiteSpace($section)) {
      continue
    }

    $part = $section -replace '^\r\n+', ''
    if ([string]::IsNullOrWhiteSpace($part) -or $part.StartsWith("--")) {
      continue
    }

    $partSections = $part -split "`r`n`r`n", 2
    if ($partSections.Count -lt 2) {
      continue
    }

    $headerText = $partSections[0]
    $contentText = $partSections[1]
    if ($contentText.EndsWith("`r`n")) {
      $contentText = $contentText.Substring(0, $contentText.Length - 2)
    }

    $partHeaders = @{}
    foreach ($headerLine in ($headerText -split "`r`n")) {
      $colonIndex = $headerLine.IndexOf(":")
      if ($colonIndex -lt 1) {
        continue
      }

      $headerName = $headerLine.Substring(0, $colonIndex).Trim()
      $headerValue = $headerLine.Substring($colonIndex + 1).Trim()
      $partHeaders[$headerName] = $headerValue
    }

    $disposition = [string]$partHeaders["Content-Disposition"]
    if ([string]::IsNullOrWhiteSpace($disposition)) {
      continue
    }

    $nameMatch = [regex]::Match(
      $disposition,
      'name=(?:"([^"]+)"|([^;]+))'
    )
    if (-not $nameMatch.Success) {
      continue
    }

    $name = if ($nameMatch.Groups[1].Success) {
      $nameMatch.Groups[1].Value
    } else {
      $nameMatch.Groups[2].Value.Trim()
    }

    $filenameMatch = [regex]::Match(
      $disposition,
      'filename=(?:"([^"]*)"|([^;]+))'
    )

    $resolvedFileName = $null
    if ($filenameMatch.Success) {
      $resolvedFileName = if ($filenameMatch.Groups[1].Success) {
        $filenameMatch.Groups[1].Value
      } else {
        $filenameMatch.Groups[2].Value.Trim()
      }
    }

    $contentBytes = $latin1.GetBytes($contentText)
    if ($filenameMatch.Success -and
        -not [string]::IsNullOrWhiteSpace($resolvedFileName)) {
      $files += [pscustomobject]@{
        Name = $name
        FileName = $resolvedFileName
        Headers = $partHeaders
        Bytes = $contentBytes
      }
      continue
    }

    $fields[$name] = [System.Text.Encoding]::UTF8.GetString($contentBytes).Trim()
  }

  return [pscustomobject]@{
    Fields = $fields
    Files = $files
  }
}

function Get-SafeName {
  param([string]$Value)

  if ([string]::IsNullOrWhiteSpace($Value)) {
    return "item"
  }

  $safe = $Value.Trim()
  $safe = $safe.Replace(([string][char]0x00C4), "Ae")
  $safe = $safe.Replace(([string][char]0x00D6), "Oe")
  $safe = $safe.Replace(([string][char]0x00DC), "Ue")
  $safe = $safe.Replace(([string][char]0x00E4), "ae")
  $safe = $safe.Replace(([string][char]0x00F6), "oe")
  $safe = $safe.Replace(([string][char]0x00FC), "ue")
  $safe = $safe.Replace(([string][char]0x00DF), "ss")

  $invalidChars = [System.IO.Path]::GetInvalidFileNameChars()
  foreach ($invalidChar in $invalidChars) {
    $safe = $safe.Replace([string]$invalidChar, "_")
  }

  $safe = ($safe -replace "\s+", "_") -replace "[^a-zA-Z0-9._-]", "_"
  $safe = $safe -replace "_{2,}", "_"
  $safe = $safe.Trim([char[]]"._")
  if ([string]::IsNullOrWhiteSpace($safe)) {
    return "item"
  }

  return $safe
}

function Get-SideLabel {
  param([string]$Side)

  switch ($Side.ToLowerInvariant()) {
    "entry" { return "Eintritt" }
    "eintritt" { return "Eintritt" }
    "exit" { return "Austritt" }
    "austritt" { return "Austritt" }
    default { return (Get-SafeName -Value $Side) }
  }
}

function Get-HddFileToken {
  param([string]$Value)

  if ([string]::IsNullOrWhiteSpace($Value)) {
    return "HDD"
  }

  $hddMatch = [regex]::Match($Value, '(?i)hdd[-_\s]*([0-9]+)')
  if ($hddMatch.Success) {
    return "HDD$($hddMatch.Groups[1].Value)"
  }

  $numberMatch = [regex]::Match($Value, '([0-9]+)')
  if ($numberMatch.Success) {
    return "HDD$($numberMatch.Groups[1].Value)"
  }

  return (Get-SafeName -Value $Value)
}

function Get-FileExtension {
  param(
    [string]$FileName,
    [string]$ContentType
  )

  $extension = [System.IO.Path]::GetExtension($FileName)
  if (-not [string]::IsNullOrWhiteSpace($extension)) {
    return $extension.ToLowerInvariant()
  }

  switch ($ContentType.ToLowerInvariant()) {
    "image/png" { return ".png" }
    "image/webp" { return ".webp" }
    "image/heic" { return ".heic" }
    default { return ".jpg" }
  }
}

function Get-NextSequenceNumberFromFileNames {
  param([string[]]$Names)

  $maxNumber = 0
  foreach ($name in $Names) {
    if ([string]::IsNullOrWhiteSpace([string]$name)) {
      continue
    }

    $extension = [System.IO.Path]::GetExtension([string]$name)
    if ($extension -notmatch '^\.(jpg|jpeg|png|webp|heic)$') {
      continue
    }

    $baseName = [System.IO.Path]::GetFileNameWithoutExtension([string]$name)
    $numberMatch = [regex]::Match($baseName, '_(\d+)$')
    if (-not $numberMatch.Success) {
      continue
    }

    $number = [int]$numberMatch.Groups[1].Value
    if ($number -gt $maxNumber) {
      $maxNumber = $number
    }
  }

  return ($maxNumber + 1)
}

function Get-NextSequenceNumber {
  param([string]$TargetDirectory)

  $names = @(
    Get-ChildItem -LiteralPath $TargetDirectory -File -ErrorAction SilentlyContinue |
      ForEach-Object { $_.Name }
  )

  return Get-NextSequenceNumberFromFileNames -Names $names
}

function Get-NextWebDavSequenceNumber {
  param([string[]]$PathSegments)

  $names = @(
    Get-WebDavFiles -PathSegments $PathSegments |
      ForEach-Object { [string]$_.Name }
  )

  return Get-NextSequenceNumberFromFileNames -Names $names
}

function New-BatchManifest {
  param(
    [string]$BatchId,
    [pscustomobject]$Metadata,
    [object[]]$SavedFiles,
    [string]$StorageMode,
    [string]$StoragePath
  )

  return [ordered]@{
    batchId = $BatchId
    uploadedAt = (Get-Date).ToUniversalTime().ToString("o")
    projectId = $Metadata.projectId
    projectName = $Metadata.projectName
    hddId = $Metadata.hddId
    hddName = $Metadata.hddName
    side = $Metadata.side
    sideLabel = $Metadata.sideLabel
    fileCount = @($SavedFiles).Count
    storageMode = $StorageMode
    storagePath = $StoragePath
    files = $SavedFiles
  }
}

function Resolve-UploadMetadata {
  param([pscustomobject]$Multipart)

  $catalog = Read-Catalog
  $projectId = [string]$Multipart.Fields["projectId"]
  $projectName = [string]$Multipart.Fields["projectName"]
  $hddId = [string]$Multipart.Fields["hddId"]
  $hddName = [string]$Multipart.Fields["hddName"]
  $side = [string]$Multipart.Fields["side"]

  $project = @($catalog.projects | Where-Object { $_.id -eq $projectId }) |
    Select-Object -First 1
  $hdd = @($catalog.hdds | Where-Object { $_.id -eq $hddId }) |
    Select-Object -First 1

  $resolvedProjectName = if ($null -ne $project) {
    [string]$project.name
  } elseif (-not [string]::IsNullOrWhiteSpace($projectName)) {
    $projectName
  } else {
    $projectId
  }

  $resolvedHddName = if ($null -ne $hdd) {
    [string]$hdd.name
  } elseif (-not [string]::IsNullOrWhiteSpace($hddName)) {
    $hddName
  } else {
    $hddId
  }

  $sideLabel = Get-SideLabel -Side $side

  return [pscustomobject]@{
    projectId = $projectId
    projectName = $resolvedProjectName
    projectFolder = (Get-SafeName -Value $resolvedProjectName)
    projectFileToken = (Get-SafeName -Value $resolvedProjectName)
    hddId = $hddId
    hddName = $resolvedHddName
    hddFolder = (Get-SafeName -Value $resolvedHddName)
    hddFileToken = (Get-HddFileToken -Value $resolvedHddName)
    side = $side
    sideLabel = $sideLabel
  }
}

function Save-BatchToLocalUpload {
  param(
    [pscustomobject]$Multipart,
    [pscustomobject]$Metadata,
    [string]$BatchId,
    [string]$DateToken
  )

  Ensure-UploadRoot

  $projectDirectory = Ensure-Directory -Path (Join-Path $UploadRoot $Metadata.projectFolder)
  $hddDirectory = Ensure-Directory -Path (Join-Path $projectDirectory $Metadata.hddFolder)
  $targetDirectory = Ensure-Directory -Path (Join-Path $hddDirectory $Metadata.sideLabel)
  $manifestDirectory = Ensure-Directory -Path (Join-Path $targetDirectory "_batches")

  $savedFiles = @()
  $counter = Get-NextSequenceNumber -TargetDirectory $targetDirectory

  foreach ($file in $Multipart.Files) {
    $originalName = [System.IO.Path]::GetFileName([string]$file.FileName)
    $contentType = [string]$file.Headers["Content-Type"]
    $extension = Get-FileExtension -FileName $originalName -ContentType $contentType
    $storedName = "{0}_{1}_{2}_{3}_{4}{5}" -f `
      $DateToken, `
      $Metadata.projectFileToken, `
      $Metadata.hddFileToken, `
      $Metadata.sideLabel, `
      $counter, `
      $extension
    $storedPath = Join-Path $targetDirectory $storedName

    [System.IO.File]::WriteAllBytes($storedPath, [byte[]]$file.Bytes)

    $savedFiles += [pscustomobject]@{
      originalName = $originalName
      storedName = $storedName
      size = [int]$file.Bytes.Length
      origin = [string]$file.Headers["X-Image-Origin"]
      sequenceNumber = $counter
    }

    $counter++
  }

  $workspaceRoot = (Resolve-Path -LiteralPath (Split-Path -Path $PSScriptRoot -Parent)).Path
  $resolvedTargetDirectory = (Resolve-Path -LiteralPath $targetDirectory).Path
  if ($resolvedTargetDirectory.StartsWith($workspaceRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    $storagePath = $resolvedTargetDirectory.Substring($workspaceRoot.Length).TrimStart('\\')
  } else {
    $storagePath = $resolvedTargetDirectory
  }
  $storagePath = $storagePath.Replace("\\", "/")

  $manifest = New-BatchManifest `
    -BatchId $BatchId `
    -Metadata $Metadata `
    -SavedFiles $savedFiles `
    -StorageMode "local" `
    -StoragePath $storagePath
  $manifestPath = Join-Path $manifestDirectory "$BatchId.json"
  $manifestJson = $manifest | ConvertTo-Json -Depth 12
  Set-Content -LiteralPath $manifestPath -Value $manifestJson -Encoding UTF8

  return [pscustomobject]@{
    batchId = $BatchId
    uploadedCount = $savedFiles.Count
    storagePath = $storagePath
    files = $savedFiles
    storageMode = "local"
  }
}

function Save-BatchToHiDrive {
  param(
    [pscustomobject]$Multipart,
    [pscustomobject]$Metadata,
    [string]$BatchId,
    [string]$DateToken
  )

  if (-not (Test-HiDriveConfigured)) {
    throw "HiDrive ist nicht konfiguriert."
  }

  $rootSegments = @(Get-HiDriveRootSegments)
  $projectSegments = @($rootSegments + @($Metadata.projectFolder))
  $hddSegments = @($projectSegments + @($Metadata.hddFolder))
  $targetSegments = @($hddSegments + @($Metadata.sideLabel))
  $manifestSegments = @($targetSegments + @("_batches"))

  [void](Ensure-HiDriveDirectory -PathSegments $projectSegments)
  [void](Ensure-HiDriveDirectory -PathSegments $hddSegments)
  [void](Ensure-HiDriveDirectory -PathSegments $targetSegments)
  [void](Ensure-HiDriveDirectory -PathSegments $manifestSegments)

  $savedFiles = @()
  $counter = Get-NextWebDavSequenceNumber -PathSegments $targetSegments

  foreach ($file in $Multipart.Files) {
    $originalName = [System.IO.Path]::GetFileName([string]$file.FileName)
    $contentType = [string]$file.Headers["Content-Type"]
    $extension = Get-FileExtension -FileName $originalName -ContentType $contentType
    $storedName = "{0}_{1}_{2}_{3}_{4}{5}" -f `
      $DateToken, `
      $Metadata.projectFileToken, `
      $Metadata.hddFileToken, `
      $Metadata.sideLabel, `
      $counter, `
      $extension

    $uploadResponse = Invoke-WebDavRequest `
      -Method "PUT" `
      -PathSegments ($targetSegments + @($storedName)) `
      -BodyBytes ([byte[]]$file.Bytes) `
      -ContentType $contentType

    if ($uploadResponse.StatusCode -notin @(200, 201, 204)) {
      throw (
        "HiDrive-Upload fuer '{0}' fehlgeschlagen (HTTP {1})." -f
        $storedName,
        $uploadResponse.StatusCode
      )
    }

    $savedFiles += [pscustomobject]@{
      originalName = $originalName
      storedName = $storedName
      size = [int]$file.Bytes.Length
      origin = [string]$file.Headers["X-Image-Origin"]
      sequenceNumber = $counter
    }

    $counter++
  }

  $storagePath = Join-StoragePath -Segments $targetSegments
  $manifest = New-BatchManifest `
    -BatchId $BatchId `
    -Metadata $Metadata `
    -SavedFiles $savedFiles `
    -StorageMode "hidrive-webdav" `
    -StoragePath $storagePath
  $manifestJson = $manifest | ConvertTo-Json -Depth 12
  $manifestResponse = Invoke-WebDavRequest `
    -Method "PUT" `
    -PathSegments ($manifestSegments + @("$BatchId.json")) `
    -BodyText $manifestJson `
    -ContentType "application/json; charset=utf-8"

  if ($manifestResponse.StatusCode -notin @(200, 201, 204)) {
    throw (
      "HiDrive-Manifest fuer Batch '{0}' konnte nicht gespeichert werden (HTTP {1})." -f
      $BatchId,
      $manifestResponse.StatusCode
    )
  }

  return [pscustomobject]@{
    batchId = $BatchId
    uploadedCount = $savedFiles.Count
    storagePath = $storagePath
    files = $savedFiles
    storageMode = "hidrive-webdav"
  }
}

function Test-StorageHealth {
  $storageMode = Get-StorageMode
  if ($storageMode -eq "local") {
    Ensure-UploadRoot
    return [pscustomobject]@{
      status = "ok"
      cloud = "connected"
      storageMode = $storageMode
      target = $UploadRoot
      message = "Lokaler Upload-Speicher bereit."
      error = ""
    }
  }

  try {
    $rootSegments = @(Get-HiDriveRootSegments)
    if ($rootSegments.Count -gt 0) {
      [void](Ensure-HiDriveDirectory -PathSegments $rootSegments)
    } else {
      $probe = Invoke-WebDavRequest `
        -Method "PROPFIND" `
        -Headers @{ Depth = "0" } `
        -BodyText (Get-WebDavPropfindBody) `
        -ContentType "application/xml; charset=utf-8"
      if ($probe.StatusCode -notin @(200, 207)) {
        throw "HiDrive-Root antwortet mit HTTP $($probe.StatusCode)."
      }
    }

    return [pscustomobject]@{
      status = "ok"
      cloud = "connected"
      storageMode = $storageMode
      target = Get-StorageTargetDescription
      message = "HiDrive erreichbar."
      error = ""
    }
  } catch {
    return [pscustomobject]@{
      status = "warn"
      cloud = "disconnected"
      storageMode = $storageMode
      target = Get-StorageTargetDescription
      message = "HiDrive nicht erreichbar."
      error = $_.Exception.Message
    }
  }
}

function Save-BatchUpload {
  param([pscustomobject]$Multipart)

  $metadata = Resolve-UploadMetadata -Multipart $Multipart
  $batchId = Get-Date -Format "yyyyMMdd-HHmmssfff"
  $dateToken = Get-Date -Format "yyMMdd"

  if ((Get-StorageMode) -eq "hidrive-webdav") {
    return Save-BatchToHiDrive `
      -Multipart $Multipart `
      -Metadata $metadata `
      -BatchId $batchId `
      -DateToken $dateToken
  }

  return Save-BatchToLocalUpload `
    -Multipart $Multipart `
    -Metadata $metadata `
    -BatchId $batchId `
    -DateToken $dateToken
}
function Resolve-ApiRequest {
  param(
    [string]$Method,
    [System.Uri]$Url,
    [hashtable]$Headers,
    [byte[]]$Body,
    [string]$RequestId
  )

  $segments = @(Get-PathSegments -Url $Url)

  if ($Method -eq "OPTIONS") {
    return New-ApiResponse -StatusCode 204 -Payload $null
  }

  if (-not (Test-ApiAuthorization -Headers $Headers)) {
    return New-ApiErrorResponse `
      -StatusCode 401 `
      -Code "unauthorized" `
      -Message "API-Token fehlt oder ist ungueltig." `
      -RequestId $RequestId
  }

  if (
    $Method -eq "POST" -and
    $segments.Count -eq 2 -and
    $segments[0] -eq "uploads" -and
    $segments[1] -eq "batches"
  ) {
    $contentType = [string]$Headers["Content-Type"]
    if ($contentType -notmatch "^multipart/form-data") {
      return New-ApiErrorResponse `
        -StatusCode 400 `
        -Code "invalid_content_type" `
        -Message "Multipart-Upload erwartet." `
        -RequestId $RequestId
    }

    $boundary = Get-MultipartBoundary -ContentType $contentType
    if ([string]::IsNullOrWhiteSpace($boundary)) {
      return New-ApiErrorResponse `
        -StatusCode 400 `
        -Code "missing_boundary" `
        -Message "Multipart-Boundary fehlt." `
        -RequestId $RequestId
    }

    try {
      $multipart = ConvertFrom-MultipartFormData -Body $Body -Boundary $boundary
    } catch {
      Write-LogEntry `
        -Level "ERROR" `
        -Message "Multipart-Upload konnte nicht verarbeitet werden." `
        -RequestId $RequestId `
        -Method $Method `
        -Path $Url.AbsolutePath `
        -StatusCode 400 `
        -Data @{
          error = $_.Exception.Message
        }
      return New-ApiErrorResponse `
        -StatusCode 400 `
        -Code "invalid_multipart" `
        -Message "Multipart-Upload konnte nicht verarbeitet werden." `
        -RequestId $RequestId
    }

    foreach ($fieldName in @("projectId", "hddId", "side")) {
      if ([string]::IsNullOrWhiteSpace([string]$multipart.Fields[$fieldName])) {
        return New-ApiErrorResponse `
          -StatusCode 400 `
          -Code "missing_field" `
          -Message "Feld '$fieldName' fehlt." `
          -RequestId $RequestId
      }
    }

    $fileCount = @($multipart.Files).Count
    if ($fileCount -lt 1) {
      return New-ApiErrorResponse `
        -StatusCode 400 `
        -Code "missing_files" `
        -Message "Keine Bilder im Batch gefunden." `
        -RequestId $RequestId
    }

    if ($fileCount -gt $MaxFilesPerBatch) {
      return New-ApiErrorResponse `
        -StatusCode 413 `
        -Code "too_many_files" `
        -Message "Zu viele Bilder im Batch." `
        -RequestId $RequestId `
        -Details @{
          maxFilesPerBatch = $MaxFilesPerBatch
          receivedFiles = $fileCount
        }
    }

    foreach ($file in $multipart.Files) {
      if (-not (Test-AllowedImageFile -File $file)) {
        return New-ApiErrorResponse `
          -StatusCode 415 `
          -Code "unsupported_media_type" `
          -Message "Nur JPG, PNG, WEBP und HEIC werden unterstuetzt." `
          -RequestId $RequestId `
          -Details @{
            fileName = [string]$file.FileName
            contentType = [string]$file.Headers["Content-Type"]
          }
      }

      if ([int]$file.Bytes.Length -gt $MaxFileBytes) {
        return New-ApiErrorResponse `
          -StatusCode 413 `
          -Code "file_too_large" `
          -Message "Mindestens eine Bilddatei ist zu gross." `
          -RequestId $RequestId `
          -Details @{
            fileName = [string]$file.FileName
            maxFileBytes = $MaxFileBytes
            actualFileBytes = [int]$file.Bytes.Length
          }
      }
    }

    try {
      $savedBatch = Save-BatchUpload -Multipart $multipart
      return New-ApiResponse -StatusCode 201 -Payload @{
        status = "ok"
        batchId = $savedBatch.batchId
        uploadedCount = $savedBatch.uploadedCount
        storagePath = $savedBatch.storagePath
        storageMode = $savedBatch.storageMode
        files = $savedBatch.files
        requestId = $RequestId
      }
    } catch {
      Write-LogEntry `
        -Level "ERROR" `
        -Message "Upload konnte nicht gespeichert werden." `
        -RequestId $RequestId `
        -Method $Method `
        -Path $Url.AbsolutePath `
        -StatusCode 500 `
        -Data @{
          error = $_.Exception.Message
        }
      return New-ApiErrorResponse `
        -StatusCode 500 `
        -Code "upload_failed" `
        -Message "Upload konnte nicht gespeichert werden." `
        -RequestId $RequestId
    }
  }

  if ($Method -ne "GET") {
    return New-ApiErrorResponse `
      -StatusCode 405 `
      -Code "method_not_allowed" `
      -Message "Nur GET, POST und OPTIONS werden unterstuetzt." `
      -RequestId $RequestId
  }

  if ($segments.Count -eq 0) {
    return New-ApiResponse -StatusCode 200 -Payload @{
      name = "KKM Capture API"
      endpoints = @(
        "/health",
        "/projects",
        "/projects/{projectId}/hdds",
        "/uploads/batches"
      )
      authRequired = (-not [string]::IsNullOrWhiteSpace($ApiToken))
      limits = @{
        maxFilesPerBatch = $MaxFilesPerBatch
        maxRequestBytes = $MaxRequestBytes
        maxFileBytes = $MaxFileBytes
      }
      storage = @{
        mode = Get-StorageMode
        target = Get-StorageTargetDescription
      }
    }
  }

  if ($segments.Count -eq 1 -and $segments[0] -eq "health") {
    $storageHealth = Test-StorageHealth
    $payload = [ordered]@{
      status = $storageHealth.status
      cloud = $storageHealth.cloud
      checkedAt = (Get-Date).ToUniversalTime().ToString("o")
      authRequired = (-not [string]::IsNullOrWhiteSpace($ApiToken))
      limits = @{
        maxFilesPerBatch = $MaxFilesPerBatch
        maxRequestBytes = $MaxRequestBytes
        maxFileBytes = $MaxFileBytes
      }
      storage = @{
        mode = $storageHealth.storageMode
        target = $storageHealth.target
      }
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$storageHealth.message)) {
      $payload.message = [string]$storageHealth.message
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$storageHealth.error)) {
      $payload.error = [string]$storageHealth.error
    }

    return New-ApiResponse -StatusCode 200 -Payload $payload
  }

  $catalog = Read-Catalog

  if ($segments.Count -eq 1 -and $segments[0] -eq "projects") {
    return New-ApiResponse -StatusCode 200 -Payload @{
      projects = @($catalog.projects)
    }
  }

  if (
    $segments.Count -eq 3 -and
    $segments[0] -eq "projects" -and
    $segments[2] -eq "hdds"
  ) {
    $projectId = $segments[1]
    $project = @($catalog.projects | Where-Object { $_.id -eq $projectId }) |
      Select-Object -First 1

    if ($null -eq $project) {
      return New-ApiErrorResponse `
        -StatusCode 404 `
        -Code "project_not_found" `
        -Message "Projekt '$projectId' wurde nicht gefunden." `
        -RequestId $RequestId
    }

    $hdds = @($catalog.hdds | Where-Object { $_.projectId -eq $projectId } |
      ForEach-Object {
        [pscustomobject]@{
          id = $_.id
          name = $_.name
          diameter = $_.diameter
          station = $_.station
        }
      })

    return New-ApiResponse -StatusCode 200 -Payload @{
      projectId = $projectId
      hdds = $hdds
    }
  }

  return New-ApiErrorResponse `
    -StatusCode 404 `
    -Code "not_found" `
    -Message "Endpunkt nicht gefunden." `
    -RequestId $RequestId
}

function Get-ReasonPhrase {
  param([int]$StatusCode)

  switch ($StatusCode) {
    401 { "Unauthorized" }
    200 { "OK" }
    201 { "Created" }
    204 { "No Content" }
    400 { "Bad Request" }
    404 { "Not Found" }
    405 { "Method Not Allowed" }
    413 { "Payload Too Large" }
    415 { "Unsupported Media Type" }
    500 { "Internal Server Error" }
    default { "OK" }
  }
}

function Write-HttpJson {
  param(
    [System.Net.Sockets.NetworkStream]$Stream,
    [int]$StatusCode,
    [object]$Payload
  )

  $bodyBytes = [byte[]]@()
  if ($null -ne $Payload -and $StatusCode -ne 204) {
    $json = $Payload | ConvertTo-Json -Depth 12
    $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes($json)
  }

  $reason = Get-ReasonPhrase -StatusCode $StatusCode
  $headers = @(
    "HTTP/1.1 $StatusCode $reason",
    "Content-Type: application/json; charset=utf-8",
    "Access-Control-Allow-Origin: *",
    "Access-Control-Allow-Methods: GET, POST, OPTIONS",
    "Access-Control-Allow-Headers: Content-Type, Accept, Authorization, X-KKM-Token",
    "Cache-Control: no-store",
    "Content-Length: $($bodyBytes.Length)",
    "Connection: close",
    "",
    ""
  ) -join "`r`n"

  $headerBytes = [System.Text.Encoding]::ASCII.GetBytes($headers)
  $Stream.Write($headerBytes, 0, $headerBytes.Length)
  if ($bodyBytes.Length -gt 0) {
    $Stream.Write($bodyBytes, 0, $bodyBytes.Length)
  }
  $Stream.Flush()
}

function Read-Request {
  param(
    [System.Net.Sockets.NetworkStream]$Stream,
    [string]$RequestId
  )

  $headerBytes = New-Object System.Collections.Generic.List[byte]
  $byteBuffer = New-Object byte[] 1
  $terminator = [byte[]](13, 10, 13, 10)
  $matched = 0

  while ($true) {
    $read = $Stream.Read($byteBuffer, 0, 1)
    if ($read -le 0) {
      break
    }

    $currentByte = $byteBuffer[0]
    $headerBytes.Add($currentByte)

    if ($currentByte -eq $terminator[$matched]) {
      $matched++
      if ($matched -eq $terminator.Length) {
        break
      }
    } else {
      $matched = if ($currentByte -eq $terminator[0]) { 1 } else { 0 }
    }

    if ($headerBytes.Count -gt 65536) {
      return [pscustomobject]@{
        ErrorResponse = (New-ApiErrorResponse `
          -StatusCode 400 `
          -Code "header_too_large" `
          -Message "Request-Header zu gross." `
          -RequestId $RequestId)
      }
    }
  }

  if ($headerBytes.Count -eq 0) {
    return $null
  }

  $headerText = [System.Text.Encoding]::ASCII.GetString($headerBytes.ToArray())
  $lines = $headerText.Split([string[]]@("`r`n"), [System.StringSplitOptions]::None)
  $requestLine = $lines[0]
  if ([string]::IsNullOrWhiteSpace($requestLine)) {
    return $null
  }

  $parts = @($requestLine -split " ")
  if ($parts.Count -lt 2) {
    return [pscustomobject]@{
      Method = "BAD"
      Target = "/"
      Headers = @{}
      Body = [byte[]]@()
    }
  }

  $headers = @{}
  for ($i = 1; $i -lt $lines.Count; $i++) {
    $line = $lines[$i]
    if ([string]::IsNullOrEmpty($line)) {
      break
    }

    $colonIndex = $line.IndexOf(":")
    if ($colonIndex -lt 1) {
      continue
    }

    $headerName = $line.Substring(0, $colonIndex).Trim()
    $headerValue = $line.Substring($colonIndex + 1).Trim()
    $headers[$headerName] = $headerValue
  }

  $contentLength = 0
  if ($headers.ContainsKey("Content-Length")) {
    [void][int]::TryParse([string]$headers["Content-Length"], [ref]$contentLength)
  }

  $body = [byte[]]@()
  if ($contentLength -gt 0) {
    if ($contentLength -gt $MaxRequestBytes) {
      return [pscustomobject]@{
        ErrorResponse = (New-ApiErrorResponse `
          -StatusCode 413 `
          -Code "request_too_large" `
          -Message "Request ist groesser als das konfigurierte Upload-Limit." `
          -RequestId $RequestId `
          -Details @{
            maxRequestBytes = $MaxRequestBytes
            actualRequestBytes = $contentLength
          })
      }
    }

    $body = New-Object byte[] $contentLength
    $offset = 0
    while ($offset -lt $contentLength) {
      $read = $Stream.Read($body, $offset, $contentLength - $offset)
      if ($read -le 0) {
        break
      }
      $offset += $read
    }

    if ($offset -lt $contentLength) {
      return [pscustomobject]@{
        ErrorResponse = (New-ApiErrorResponse `
          -StatusCode 400 `
          -Code "incomplete_body" `
          -Message "Request-Body unvollstaendig." `
          -RequestId $RequestId)
      }
    }
  }

  return [pscustomobject]@{
    Method = $parts[0].ToUpperInvariant()
    Target = $parts[1]
    Headers = $headers
    Body = $body
  }
}

function New-RequestUri {
  param([string]$Target)

  if ($Target -match "^https?://") {
    return [System.Uri]::new($Target)
  }

  return [System.Uri]::new("http://localhost:$Port$Target")
}

$listener = [System.Net.Sockets.TcpListener]::new(
  [System.Net.IPAddress]::Any,
  $Port
)
$listener.Start()

Write-Host "KKM Capture API laeuft auf http://0.0.0.0:$Port/"
Write-Host "Lokal erreichbar unter http://localhost:$Port/"
Write-Host "Datenquelle: $DataPath"
Write-Host "Storage-Modus: $(Get-StorageMode)"
Write-Host "Storage-Ziel: $(Get-StorageTargetDescription)"
Write-Host "Upload-Ziel lokal: $UploadRoot"
Write-Host "Logdatei: $LogPath"
Write-Host "Beenden mit Strg+C"

Write-LogEntry `
  -Level "INFO" `
  -Message "Server gestartet." `
  -StatusCode 200 `
  -Data @{
    port = $Port
    dataPath = $DataPath
    uploadRoot = $UploadRoot
    logPath = $LogPath
    storageMode = Get-StorageMode
    storageTarget = Get-StorageTargetDescription
    hiDriveBaseUrl = $HiDriveBaseUrl
    authRequired = (-not [string]::IsNullOrWhiteSpace($ApiToken))
    maxFilesPerBatch = $MaxFilesPerBatch
    maxRequestBytes = $MaxRequestBytes
    maxFileBytes = $MaxFileBytes
  }

try {
  while ($true) {
    $client = $listener.AcceptTcpClient()
    $stream = $null
    $request = $null
    $requestId = New-RequestId
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    try {
      $stream = $client.GetStream()
      $request = Read-Request -Stream $stream -RequestId $requestId

      if ($null -eq $request) {
        continue
      }

      if ($null -ne $request.ErrorResponse) {
        $response = $request.ErrorResponse
      } else {
        try {
          $uri = New-RequestUri -Target $request.Target
          $response = Resolve-ApiRequest `
            -Method $request.Method `
            -Url $uri `
            -Headers $request.Headers `
            -Body $request.Body `
            -RequestId $requestId
        } catch {
          Write-LogEntry `
            -Level "ERROR" `
            -Message "Ungueltige Anfrage." `
            -RequestId $requestId `
            -Method $request.Method `
            -Path $request.Target `
            -StatusCode 400 `
            -Data @{
              error = $_.Exception.Message
            }
          $response = New-ApiErrorResponse `
            -StatusCode 400 `
            -Code "invalid_request" `
            -Message "Ungueltige Anfrage." `
            -RequestId $requestId
        }
      }

      Write-HttpJson `
        -Stream $stream `
        -StatusCode $response.StatusCode `
        -Payload $response.Payload

      $stopwatch.Stop()
      $methodForLog = [string]$request.Method
      $pathForLog = [string]$request.Target
      $contentLengthForLog = if ($null -ne $request.Body) {
        $request.Body.Length
      } else {
        0
      }
      $payloadMessage = ""
      if ($response.Payload -is [hashtable] -or $response.Payload -is [pscustomobject]) {
        $payloadMessage = [string]$response.Payload.message
        if ([string]::IsNullOrWhiteSpace($payloadMessage)) {
          $payloadMessage = [string]$response.Payload.status
        }
      }
      $logLevel = if ($response.StatusCode -ge 500) {
        "ERROR"
      } elseif ($response.StatusCode -ge 400) {
        "WARN"
      } else {
        "INFO"
      }
      $remoteClient = try {
        $client.Client.RemoteEndPoint.ToString()
      } catch {
        ""
      }
      Write-LogEntry `
        -Level $logLevel `
        -Message $payloadMessage `
        -RequestId $requestId `
        -Method $methodForLog `
        -Path $pathForLog `
        -StatusCode $response.StatusCode `
        -Data @{
          durationMs = $stopwatch.ElapsedMilliseconds
          contentLength = $contentLengthForLog
          client = $remoteClient
        }
    } catch {
      $stopwatch.Stop()
      if ($null -ne $stream) {
        $response = New-ApiErrorResponse `
          -StatusCode 500 `
          -Code "internal_error" `
          -Message "Interner Serverfehler." `
          -RequestId $requestId
        Write-HttpJson -Stream $stream -StatusCode $response.StatusCode -Payload $response.Payload
      }
      Write-LogEntry `
        -Level "ERROR" `
        -Message "Interner Serverfehler." `
        -RequestId $requestId `
        -Method ([string]$request.Method) `
        -Path ([string]$request.Target) `
        -StatusCode 500 `
        -Data @{
          durationMs = $stopwatch.ElapsedMilliseconds
          error = $_.Exception.Message
        }
    } finally {
      $client.Close()
    }
  }
} finally {
  $listener.Stop()
  Write-LogEntry -Level "INFO" -Message "Server gestoppt." -StatusCode 200
}
