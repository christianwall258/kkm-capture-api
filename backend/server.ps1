param(
  [int]$Port = $(if ([string]::IsNullOrWhiteSpace($env:PORT)) { 8787 } else { [int]$env:PORT }),
  [string]$UploadRoot = (Join-Path $PSScriptRoot "uploads"),
  [string]$LogPath = (Join-Path $PSScriptRoot "logs\server.log"),
  [string]$ApiToken = $env:KKM_API_TOKEN,
  [int]$MaxFilesPerBatch = 10,
  [int]$MaxRequestBytes = 26214400,
  [int]$MaxFileBytes = 8388608,
  [string]$HiDriveBaseUrl = $(if ([string]::IsNullOrWhiteSpace($env:HIDRIVE_BASE_URL)) { "https://webdav.hidrive.ionos.com" } else { $env:HIDRIVE_BASE_URL }),
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

function Get-StorageBaseSegments {
  if ((Get-StorageMode) -eq "hidrive-webdav") {
    return @(Get-HiDriveRootSegments)
  }

  return @()
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

  foreach ($invalidChar in [System.IO.Path]::GetInvalidFileNameChars()) {
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

function Get-StorageFolderName {
  param([string]$Value)

  if ([string]::IsNullOrWhiteSpace($Value)) {
    return "item"
  }

  $name = $Value.Trim()
  foreach ($invalidChar in [System.IO.Path]::GetInvalidFileNameChars()) {
    $name = $name.Replace([string]$invalidChar, "_")
  }
  $name = $name.Replace('/', '_').Replace('\', '_')
  $name = ($name -replace '\s{2,}', ' ').Trim()

  if ([string]::IsNullOrWhiteSpace($name)) {
    return "item"
  }

  return $name
}

function Resolve-SideValue {
  param([string]$Side)

  $normalizedSide = if ($null -eq $Side) { "" } else { $Side }

  switch ($normalizedSide.Trim().ToLowerInvariant()) {
    "entry" { return "entry" }
    "eintritt" { return "entry" }
    "exit" { return "exit" }
    "austritt" { return "exit" }
    default { throw "Seite '$Side' ist ungueltig." }
  }
}

function Get-SideLabel {
  param([string]$Side)

  switch (Resolve-SideValue -Side $Side) {
    "entry" { return "Eintritt" }
    "exit" { return "Austritt" }
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

  $normalizedContentType = if ($null -eq $ContentType) { "" } else { $ContentType }

  switch ($normalizedContentType.ToLowerInvariant()) {
    "image/png" { return ".png" }
    "image/webp" { return ".webp" }
    "image/heic" { return ".heic" }
    default { return ".jpg" }
  }
}

function Get-LocalStorageDirectoryPath {
  param(
    [string[]]$PathSegments = @(),
    [switch]$EnsureExists
  )

  if ($EnsureExists) {
    Ensure-UploadRoot
  }

  $targetPath = $UploadRoot
  foreach ($segment in $PathSegments) {
    if ([string]::IsNullOrWhiteSpace([string]$segment)) {
      continue
    }
    $targetPath = Join-Path $targetPath $segment
  }

  if ($EnsureExists) {
    Ensure-Directory -Path $targetPath | Out-Null
  }

  return $targetPath
}

function Get-LocalStorageDirectories {
  param([string[]]$PathSegments = @())

  $targetPath = Get-LocalStorageDirectoryPath -PathSegments $PathSegments
  if (-not (Test-Path -LiteralPath $targetPath -PathType Container)) {
    return @()
  }

  return @(
    Get-ChildItem -LiteralPath $targetPath -Directory -ErrorAction SilentlyContinue |
      Sort-Object -Property Name |
      ForEach-Object {
        [pscustomobject]@{
          Name = $_.Name
        }
      }
  )
}

function Get-LocalStorageFiles {
  param([string[]]$PathSegments = @())

  $targetPath = Get-LocalStorageDirectoryPath -PathSegments $PathSegments
  if (-not (Test-Path -LiteralPath $targetPath -PathType Container)) {
    return @()
  }

  return @(
    Get-ChildItem -LiteralPath $targetPath -File -ErrorAction SilentlyContinue |
      Sort-Object -Property Name |
      ForEach-Object {
        [pscustomobject]@{
          Name = $_.Name
        }
      }
  )
}

function Test-LocalStorageDirectoryExists {
  param([string[]]$PathSegments = @())

  $targetPath = Get-LocalStorageDirectoryPath -PathSegments $PathSegments
  return Test-Path -LiteralPath $targetPath -PathType Container
}

function Save-LocalStorageFile {
  param(
    [string[]]$PathSegments = @(),
    [string]$FileName,
    [byte[]]$ContentBytes
  )

  $targetDirectory = Get-LocalStorageDirectoryPath -PathSegments $PathSegments -EnsureExists
  $targetPath = Join-Path $targetDirectory $FileName
  [System.IO.File]::WriteAllBytes($targetPath, [byte[]]$ContentBytes)
}

function Save-LocalStorageText {
  param(
    [string[]]$PathSegments = @(),
    [string]$FileName,
    [string]$ContentText
  )

  $targetDirectory = Get-LocalStorageDirectoryPath -PathSegments $PathSegments -EnsureExists
  $targetPath = Join-Path $targetDirectory $FileName
  [System.IO.File]::WriteAllText(
    $targetPath,
    $ContentText,
    [System.Text.UTF8Encoding]::new($false)
  )
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

function Get-WebDavDirectoryListing {
  param([string[]]$PathSegments = @())

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
    throw "WebDAV-Verzeichnisliste konnte nicht geladen werden (HTTP $($response.StatusCode))."
  }

  if ([string]::IsNullOrWhiteSpace($response.Body)) {
    return @()
  }

  try {
    [xml]$xml = $response.Body
  } catch {
    throw "WebDAV-Verzeichnisliste ist kein gueltiges XML."
  }

  $namespaceManager = [System.Xml.XmlNamespaceManager]::new($xml.NameTable)
  $namespaceManager.AddNamespace("d", "DAV:")
  $responses = $xml.SelectNodes("//d:response", $namespaceManager)
  $requestedPath = (New-WebDavUri -PathSegments $PathSegments).AbsolutePath.TrimEnd('/')
  $items = @()

  foreach ($node in $responses) {
    $hrefNode = $node.SelectSingleNode("d:href", $namespaceManager)
    if ($null -eq $hrefNode) {
      continue
    }

    $hrefText = [string]$hrefNode.InnerText
    $hrefPath = $hrefText
    try {
      if ($hrefText -match '^https?://') {
        $hrefPath = ([System.Uri]::new($hrefText)).AbsolutePath
      }
    } catch {
      $hrefPath = $hrefText
    }

    if ($hrefPath.TrimEnd('/') -eq $requestedPath) {
      continue
    }

    $name = Get-WebDavNameFromHref -Href $hrefText
    if ([string]::IsNullOrWhiteSpace($name)) {
      continue
    }

    $isCollection = $null -ne $node.SelectSingleNode(
      ".//d:resourcetype/d:collection",
      $namespaceManager
    )

    $items += [pscustomobject]@{
      Name = $name
      IsDirectory = $isCollection
    }
  }

  return $items
}

function Get-WebDavDirectories {
  param([string[]]$PathSegments = @())

  return @(
    Get-WebDavDirectoryListing -PathSegments $PathSegments |
      Where-Object { $_.IsDirectory } |
      Sort-Object -Property Name |
      ForEach-Object {
        [pscustomobject]@{
          Name = $_.Name
        }
      }
  )
}

function Get-WebDavFiles {
  param([string[]]$PathSegments = @())

  return @(
    Get-WebDavDirectoryListing -PathSegments $PathSegments |
      Where-Object { -not $_.IsDirectory } |
      Sort-Object -Property Name |
      ForEach-Object {
        [pscustomobject]@{
          Name = $_.Name
        }
      }
  )
}

function Test-WebDavDirectoryExists {
  param([string[]]$PathSegments = @())

  $response = Invoke-WebDavRequest `
    -Method "PROPFIND" `
    -PathSegments $PathSegments `
    -Headers @{ Depth = "0" } `
    -BodyText (Get-WebDavPropfindBody) `
    -ContentType "application/xml; charset=utf-8"

  if ($response.StatusCode -eq 404) {
    return $false
  }

  if ($response.StatusCode -notin @(200, 207)) {
    throw "WebDAV-Verzeichnis konnte nicht geprueft werden (HTTP $($response.StatusCode))."
  }

  return $true
}

function Save-WebDavFile {
  param(
    [string[]]$PathSegments = @(),
    [string]$FileName,
    [byte[]]$ContentBytes,
    [string]$ContentType
  )

  $response = Invoke-WebDavRequest `
    -Method "PUT" `
    -PathSegments (@($PathSegments) + @($FileName)) `
    -BodyBytes $ContentBytes `
    -ContentType $ContentType

  if ($response.StatusCode -notin @(200, 201, 204)) {
    throw "WebDAV-Datei '$FileName' konnte nicht gespeichert werden (HTTP $($response.StatusCode))."
  }
}

function Save-WebDavText {
  param(
    [string[]]$PathSegments = @(),
    [string]$FileName,
    [string]$ContentText
  )

  Save-WebDavFile `
    -PathSegments $PathSegments `
    -FileName $FileName `
    -ContentBytes ([System.Text.Encoding]::UTF8.GetBytes($ContentText)) `
    -ContentType "application/json; charset=utf-8"
}

function Ensure-StorageDirectory {
  param([string[]]$PathSegments = @())

  if ((Get-StorageMode) -eq "hidrive-webdav") {
    Ensure-HiDriveDirectory -PathSegments $PathSegments | Out-Null
    return (Join-StoragePath -Segments $PathSegments)
  }

  return (Get-LocalStorageDirectoryPath -PathSegments $PathSegments -EnsureExists)
}

function Get-StorageDirectories {
  param([string[]]$PathSegments = @())

  if ((Get-StorageMode) -eq "hidrive-webdav") {
    return @(Get-WebDavDirectories -PathSegments $PathSegments)
  }

  return @(Get-LocalStorageDirectories -PathSegments $PathSegments)
}

function Get-StorageFiles {
  param([string[]]$PathSegments = @())

  if ((Get-StorageMode) -eq "hidrive-webdav") {
    return @(Get-WebDavFiles -PathSegments $PathSegments)
  }

  return @(Get-LocalStorageFiles -PathSegments $PathSegments)
}

function Test-StorageDirectoryExists {
  param([string[]]$PathSegments = @())

  if ($PathSegments.Count -eq 0) {
    return $true
  }

  if ((Get-StorageMode) -eq "hidrive-webdav") {
    return Test-WebDavDirectoryExists -PathSegments $PathSegments
  }

  return Test-LocalStorageDirectoryExists -PathSegments $PathSegments
}

function Save-StorageFile {
  param(
    [string[]]$PathSegments = @(),
    [string]$FileName,
    [byte[]]$ContentBytes,
    [string]$ContentType
  )

  if ((Get-StorageMode) -eq "hidrive-webdav") {
    Save-WebDavFile `
      -PathSegments $PathSegments `
      -FileName $FileName `
      -ContentBytes $ContentBytes `
      -ContentType $ContentType
    return
  }

  Save-LocalStorageFile `
    -PathSegments $PathSegments `
    -FileName $FileName `
    -ContentBytes $ContentBytes
}

function Save-StorageText {
  param(
    [string[]]$PathSegments = @(),
    [string]$FileName,
    [string]$ContentText
  )

  if ((Get-StorageMode) -eq "hidrive-webdav") {
    Save-WebDavText `
      -PathSegments $PathSegments `
      -FileName $FileName `
      -ContentText $ContentText
    return
  }

  Save-LocalStorageText `
    -PathSegments $PathSegments `
    -FileName $FileName `
    -ContentText $ContentText
}

function Get-StorageRelativePath {
  param([string[]]$PathSegments = @())

  if ((Get-StorageMode) -eq "hidrive-webdav") {
    return (Join-StoragePath -Segments $PathSegments)
  }

  $targetDirectory = Get-LocalStorageDirectoryPath -PathSegments $PathSegments -EnsureExists
  $workspaceRoot = (Resolve-Path -LiteralPath (Split-Path -Path $PSScriptRoot -Parent)).Path
  $resolvedTargetDirectory = (Resolve-Path -LiteralPath $targetDirectory).Path
  if ($resolvedTargetDirectory.StartsWith($workspaceRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    return $resolvedTargetDirectory.Substring($workspaceRoot.Length).TrimStart('\').Replace("\", "/")
  }

  return $resolvedTargetDirectory.Replace("\", "/")
}

function Get-NextSequenceNumber {
  param([string[]]$TargetPathSegments = @())

  $maxNumber = 0
  $existingFiles = Get-StorageFiles -PathSegments $TargetPathSegments |
    Where-Object { $_.Name -match '\.(jpg|jpeg|png|webp|heic)$' }

  foreach ($existingFile in $existingFiles) {
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension([string]$existingFile.Name)
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

function ConvertTo-ProjectRecords {
  param([object[]]$Directories = @())

  return @(
    $Directories |
      Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_.Name) } |
      Sort-Object -Property { $_.Name.ToLowerInvariant() } |
      ForEach-Object {
        [pscustomobject]@{
          id = [string]$_.Name
          name = [string]$_.Name
        }
      }
  )
}

function ConvertTo-HddRecords {
  param([object[]]$Directories = @())

  return @(
    $Directories |
      Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_.Name) } |
      Sort-Object -Property { $_.Name.ToLowerInvariant() } |
      ForEach-Object {
        [pscustomobject]@{
          id = [string]$_.Name
          name = [string]$_.Name
        }
      }
  )
}

function Get-ProjectRecordsFromStorage {
  $directories = Get-StorageDirectories -PathSegments (Get-StorageBaseSegments)
  return ConvertTo-ProjectRecords -Directories $directories
}

function Get-HddRecordsFromStorage {
  param([string]$ProjectId)

  $projectSegments = @((Get-StorageBaseSegments) + @($ProjectId))
  if (-not (Test-StorageDirectoryExists -PathSegments $projectSegments)) {
    return $null
  }

  $directories = Get-StorageDirectories -PathSegments $projectSegments
  return ConvertTo-HddRecords -Directories $directories
}

function Test-StorageHealth {
  if ((Get-StorageMode) -eq "local") {
    Ensure-UploadRoot
    return [pscustomobject]@{
      status = "ok"
      cloud = "connected"
      message = "Lokaler Speicher bereit."
      storageMode = "local"
      storageTarget = Get-StorageTargetDescription
    }
  }

  try {
    Ensure-HiDriveDirectory -PathSegments (Get-HiDriveRootSegments) | Out-Null
    [void](Get-StorageDirectories -PathSegments (Get-HiDriveRootSegments))
    return [pscustomobject]@{
      status = "ok"
      cloud = "connected"
      message = "HiDrive erreichbar."
      storageMode = "hidrive-webdav"
      storageTarget = Get-StorageTargetDescription
    }
  } catch {
    return [pscustomobject]@{
      status = "warn"
      cloud = "disconnected"
      message = "HiDrive nicht erreichbar."
      storageMode = "hidrive-webdav"
      storageTarget = Get-StorageTargetDescription
      error = $_.Exception.Message
    }
  }
}

function Resolve-UploadMetadata {
  param([pscustomobject]$Multipart)

  $projectId = ([string]$Multipart.Fields["projectId"]).Trim()
  $projectName = ([string]$Multipart.Fields["projectName"]).Trim()
  $hddId = ([string]$Multipart.Fields["hddId"]).Trim()
  $hddName = ([string]$Multipart.Fields["hddName"]).Trim()
  $side = [string]$Multipart.Fields["side"]

  $resolvedProjectName = if (-not [string]::IsNullOrWhiteSpace($projectName)) {
    $projectName
  } else {
    $projectId
  }
  $resolvedHddName = if (-not [string]::IsNullOrWhiteSpace($hddName)) {
    $hddName
  } else {
    $hddId
  }

  if ([string]::IsNullOrWhiteSpace($resolvedProjectName)) {
    throw "Projekt fehlt."
  }
  if ([string]::IsNullOrWhiteSpace($resolvedHddName)) {
    throw "HDD fehlt."
  }

  $resolvedSide = Resolve-SideValue -Side $side
  $sideLabel = Get-SideLabel -Side $resolvedSide

  return [pscustomobject]@{
    projectId = if ([string]::IsNullOrWhiteSpace($projectId)) { $resolvedProjectName } else { $projectId }
    projectName = $resolvedProjectName
    projectFolder = (Get-StorageFolderName -Value $resolvedProjectName)
    projectFileToken = (Get-SafeName -Value $resolvedProjectName)
    hddId = if ([string]::IsNullOrWhiteSpace($hddId)) { $resolvedHddName } else { $hddId }
    hddName = $resolvedHddName
    hddFolder = (Get-StorageFolderName -Value $resolvedHddName)
    hddFileToken = (Get-HddFileToken -Value $resolvedHddName)
    side = $resolvedSide
    sideLabel = $sideLabel
  }
}

function Save-BatchUpload {
  param([pscustomobject]$Multipart)

  $metadata = Resolve-UploadMetadata -Multipart $Multipart
  $batchId = Get-Date -Format "yyyyMMdd-HHmmssfff"
  $dateToken = Get-Date -Format "yyMMdd"

  $projectSegments = @((Get-StorageBaseSegments) + @($metadata.projectFolder))
  $hddSegments = @($projectSegments + @($metadata.hddFolder))
  $targetSegments = @($hddSegments + @($metadata.sideLabel))
  $manifestSegments = @($targetSegments + @("_batches"))

  Ensure-StorageDirectory -PathSegments $projectSegments | Out-Null
  Ensure-StorageDirectory -PathSegments $hddSegments | Out-Null
  Ensure-StorageDirectory -PathSegments $targetSegments | Out-Null
  Ensure-StorageDirectory -PathSegments $manifestSegments | Out-Null

  $savedFiles = @()
  $counter = Get-NextSequenceNumber -TargetPathSegments $targetSegments

  foreach ($file in $Multipart.Files) {
    $originalName = [System.IO.Path]::GetFileName([string]$file.FileName)
    $contentType = [string]$file.Headers["Content-Type"]
    $extension = Get-FileExtension -FileName $originalName -ContentType $contentType
    $storedName = "{0}_{1}_{2}_{3}_{4}{5}" -f `
      $dateToken, `
      $metadata.projectFileToken, `
      $metadata.hddFileToken, `
      $metadata.sideLabel, `
      $counter, `
      $extension

    Save-StorageFile `
      -PathSegments $targetSegments `
      -FileName $storedName `
      -ContentBytes ([byte[]]$file.Bytes) `
      -ContentType $contentType

    $savedFiles += [pscustomobject]@{
      originalName = $originalName
      storedName = $storedName
      size = [int]$file.Bytes.Length
      origin = [string]$file.Headers["X-Image-Origin"]
      sequenceNumber = $counter
      storagePath = (Join-StoragePath -Segments (@($targetSegments) + @($storedName)))
    }

    $counter++
  }

  $manifest = [ordered]@{
    batchId = $batchId
    uploadedAt = (Get-Date).ToUniversalTime().ToString("o")
    storageMode = Get-StorageMode
    projectId = $metadata.projectId
    projectName = $metadata.projectName
    hddId = $metadata.hddId
    hddName = $metadata.hddName
    side = $metadata.side
    sideLabel = $metadata.sideLabel
    fileCount = $savedFiles.Count
    files = $savedFiles
  }

  $manifestJson = $manifest | ConvertTo-Json -Depth 12
  Save-StorageText `
    -PathSegments $manifestSegments `
    -FileName "$batchId.json" `
    -ContentText $manifestJson

  return [pscustomobject]@{
    batchId = $batchId
    uploadedCount = $savedFiles.Count
    storagePath = (Get-StorageRelativePath -PathSegments $targetSegments)
    files = $savedFiles
  }
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

    foreach ($descriptor in @(
      @{ key = "project"; value = [string]$multipart.Fields["projectId"]; fallback = [string]$multipart.Fields["projectName"] },
      @{ key = "hdd"; value = [string]$multipart.Fields["hddId"]; fallback = [string]$multipart.Fields["hddName"] }
    )) {
      $primaryValue = if ($null -eq $descriptor.value) { "" } else { [string]$descriptor.value }
      $fallbackValue = if ($null -eq $descriptor.fallback) { "" } else { [string]$descriptor.fallback }

      if ([string]::IsNullOrWhiteSpace($primaryValue.Trim()) -and
          [string]::IsNullOrWhiteSpace($fallbackValue.Trim())) {
        return New-ApiErrorResponse `
          -StatusCode 400 `
          -Code "missing_field" `
          -Message "Feld '$($descriptor.key)' fehlt." `
          -RequestId $RequestId
      }
    }

    if ([string]::IsNullOrWhiteSpace(([string]$multipart.Fields["side"]).Trim())) {
      return New-ApiErrorResponse `
        -StatusCode 400 `
        -Code "missing_field" `
        -Message "Feld 'side' fehlt." `
        -RequestId $RequestId
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
      storageMode = Get-StorageMode
      storageTarget = Get-StorageTargetDescription
      limits = @{
        maxFilesPerBatch = $MaxFilesPerBatch
        maxRequestBytes = $MaxRequestBytes
        maxFileBytes = $MaxFileBytes
      }
    }
  }

  if ($segments.Count -eq 1 -and $segments[0] -eq "health") {
    $health = Test-StorageHealth
    return New-ApiResponse -StatusCode 200 -Payload @{
      status = $health.status
      cloud = $health.cloud
      message = $health.message
      checkedAt = (Get-Date).ToUniversalTime().ToString("o")
      storageMode = $health.storageMode
      storageTarget = $health.storageTarget
      authRequired = (-not [string]::IsNullOrWhiteSpace($ApiToken))
      limits = @{
        maxFilesPerBatch = $MaxFilesPerBatch
        maxRequestBytes = $MaxRequestBytes
        maxFileBytes = $MaxFileBytes
      }
    }
  }

  if ($segments.Count -eq 1 -and $segments[0] -eq "projects") {
    try {
      $projects = Get-ProjectRecordsFromStorage
      return New-ApiResponse -StatusCode 200 -Payload @{
        projects = @($projects)
      }
    } catch {
      Write-LogEntry `
        -Level "WARN" `
        -Message "Projektordner konnten nicht geladen werden." `
        -RequestId $RequestId `
        -Method $Method `
        -Path $Url.AbsolutePath `
        -StatusCode 503 `
        -Data @{
          error = $_.Exception.Message
        }
      return New-ApiErrorResponse `
        -StatusCode 503 `
        -Code "projects_unavailable" `
        -Message "Projektordner konnten nicht geladen werden." `
        -RequestId $RequestId
    }
  }

  if (
    $segments.Count -eq 3 -and
    $segments[0] -eq "projects" -and
    $segments[2] -eq "hdds"
  ) {
    $projectId = $segments[1]
    $projectSegments = @((Get-StorageBaseSegments) + @($projectId))

    try {
      if (-not (Test-StorageDirectoryExists -PathSegments $projectSegments)) {
        return New-ApiErrorResponse `
          -StatusCode 404 `
          -Code "project_not_found" `
          -Message "Projekt '$projectId' wurde nicht gefunden." `
          -RequestId $RequestId
      }

      $hdds = Get-HddRecordsFromStorage -ProjectId $projectId
      return New-ApiResponse -StatusCode 200 -Payload @{
        projectId = $projectId
        hdds = @($hdds)
      }
    } catch {
      Write-LogEntry `
        -Level "WARN" `
        -Message "HDD-Ordner konnten nicht geladen werden." `
        -RequestId $RequestId `
        -Method $Method `
        -Path $Url.AbsolutePath `
        -StatusCode 503 `
        -Data @{
          projectId = $projectId
          error = $_.Exception.Message
        }
      return New-ApiErrorResponse `
        -StatusCode 503 `
        -Code "hdds_unavailable" `
        -Message "HDD-Ordner konnten nicht geladen werden." `
        -RequestId $RequestId
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
    200 { "OK" }
    201 { "Created" }
    204 { "No Content" }
    400 { "Bad Request" }
    401 { "Unauthorized" }
    404 { "Not Found" }
    405 { "Method Not Allowed" }
    413 { "Payload Too Large" }
    415 { "Unsupported Media Type" }
    500 { "Internal Server Error" }
    503 { "Service Unavailable" }
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
Write-Host "Upload-Ziel: $(Get-StorageTargetDescription)"
Write-Host "Storage-Modus: $(Get-StorageMode)"
Write-Host "Logdatei: $LogPath"
Write-Host "Beenden mit Strg+C"

Write-LogEntry `
  -Level "INFO" `
  -Message "Server gestartet." `
  -StatusCode 200 `
  -Data @{
    port = $Port
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
