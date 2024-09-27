# In this script, we read the contents of the  .version.yml  file, update the  PACKAGE_RELEASE  value with the new version, and write the updated content back to the file. 
# The  $env:PACKAGE_RELEASE  variable is available in the script because we set it in the  bump-version-env.ps1  script. 

param (
    [string]$release
)

# Read the .version.yml file
$versionFile = ".version.yml"
$versionContent = Get-Content $versionFile -Raw

# Update the version in the YAML content
$newVersionContent = [regex]::Replace($versionContent, 'PACKAGE_RELEASE:\s*\d+\.\d+\.\d+', "PACKAGE_RELEASE: $release")

# Write the updated content back to the .version.yml file
Set-Content -Path $versionFile -Value $newVersionContent

Write-Output "Version updated in $versionFile to $env:PACKAGE_RELEASE"
