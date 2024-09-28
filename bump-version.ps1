param (
    [string]$branch
)

# Read the .version.yml file
$versionFile = ".version.yml"
$versionContent = Get-Content $versionFile -Raw

# Extract the current version numbers
$currentRelease = [regex]::Match($versionContent, 'PACKAGE_RELEASE:\s*(\d+\.\d+\.\d+)').Groups[1].Value

# Split the version into its components
$versionParts = $currentRelease -split '\.'
$major = [int]$versionParts[0]
$minor = [int]$versionParts[1]
$build = [int]$versionParts[2]

# Update the version based on the branch
if ($branch -eq "develop") {
    $build++
} elseif ($branch -eq "master") {
    $minor++
    $build = 0
}

# Construct the new version
$newRelease = "$major.$minor.$build"

# Update the version in the YAML content
$newVersionContent = [regex]::Replace($versionContent, 'PACKAGE_RELEASE:\s*\d+\.\d+\.\d+', "PACKAGE_RELEASE: $newRelease")

# Write the updated content back to the .version.yml file
Set-Content -Path $versionFile -Value $newVersionContent

Write-Output "Version updated to $newRelease"

$env:PACKAGE_RELEASE = $newRelease
