# --- GLOBAL CONFIGURATION ---
$RCLONE_EXE   = "E:\Projects\rclone\1.73.0\rclone.exe"
$BASE_LOCAL   = "E:\Data"
$REMOTE_NAME  = "mssp_czodata"
$TIMESTAMP    = Get-Date -Format "yyMMdd-HHmm"

# --- TEST SETTINGS ---
$DryRun       = $false  
$DebugLogging = $true  

# --- PARAMETERS ---
$L1_MIN_AGE      = "15m"      
$L1_MOVE_AGE     = "25h"     
$PURGE_MONTH_AGE = 3        

# --- HELPER FUNCTION ---
# Converts Hash Tables (Splatting) to a string for Dry-Run visibility
function Get-ArgumentsString($Table) {
    $out = @()
    foreach ($key in $Table.Keys) {
        if ($Table[$key] -is [bool]) { if ($Table[$key]) { $out += "--$key" } }
        else { $out += "--$key $($Table[$key])" }
    }
    return $out -join " "
}

# --- THE STABILITY PARAMETERS ---
$CommonArgs = @{
    "update"            = $true
    "retries"           = 10
    "low-level-retries" = 20
    "progress"          = $true
    "checkers"          = 2
    "transfers"         = 2
    "tpslimit"          = 4
    "tpslimit-burst"    = 4
    
}

# --- PROJECT STRUCTURE DEFINITION ---
$Sites = @{
    "Bahada" = @{ "Projects" = @( @{ Name = "CR3000"; L1Folders = @("EddyCovariance_ts", "EddyCovariance_ts_2", "Flux", "SoilSensor_CS650", "TowerClimate_met") } ) }
    "Pecan5R" = @{
        "Projects" = @(
            @{ Name = "CR6-Above"; L1Folders = @("Time_Series", "Config_Setting_Notes", "Const_Table", "CPIStatus", "Diagnostic", "Flux_AmeriFluxFormat", "Flux_Notes", "System_Operatn_Notes") },
            @{ Name = "CR1000X-AddSen"; L1Folders = @("Additional_Sensors") },
            @{ Name = "CR1000X-Profile"; L1Folders = @("CalAvg", "RawData", "IntAvg", "message_log", "SiteAvg", "TimeInfo") }
        )
    }
    "RedLake" = @{ "Projects" = @( @{ Name = "CR3000"; L1Folders = @("Biomet", "BiometConstants", "SoilSensor_CS650", "VariableChecks") } ) }
}

# --- ENGINE ---

if ($DryRun) { Write-Host "--- DRY RUN ENABLED: NO ACTIONS WILL BE TAKEN ---" -ForegroundColor Black -BackgroundColor White }

foreach ($SiteName in $Sites.Keys) {
    foreach ($Proj in $Sites[$SiteName].Projects) {
        $ProjName = $Proj.Name
        Write-Host "`n[SITE: $SiteName | PROJ: $ProjName]" -ForegroundColor Cyan

        # 1. HANDLE L0 DATA (MOVE)
        $L0_Source = Join-Path $BASE_LOCAL "$SiteName\$ProjName\L0"
        if (Test-Path $L0_Source) {
            
            if ($DryRun) { 
                $fullArgs = "$(Get-ArgumentsString $CommonArgs)"
                Write-Host "[SIMULATE] L0 Move: $RCLONE_EXE move `"$L0_Source`" `"${REMOTE_NAME}:$SiteName/$ProjName/L0`" $fullArgs" -ForegroundColor Gray 
            } 
            else { & $RCLONE_EXE move "$L0_Source" "${REMOTE_NAME}:$SiteName/$ProjName/L0" @CommonArgs @l0Specific }
        }

        # 2. HANDLE L1 & LOGS
        $TargetSubFolders = @(); if ($Proj.L1Folders) { $TargetSubFolders += $Proj.L1Folders }; $TargetSubFolders += "logs"

        foreach ($Sub in $TargetSubFolders) {
            $LocalSubPath = if ($Sub -eq "logs") { "$SiteName\$ProjName\logs" } else { "$SiteName\$ProjName\L1\$Sub" }
            $FullLocalPath = Join-Path $BASE_LOCAL $LocalSubPath

            if (Test-Path $FullLocalPath) {
                $IsMoveType = $Sub -match "_ts|Time_Series|RawData|CalAvg"
                $Action     = if ($IsMoveType) { "move" } else { "copy" }
                $Age        = if ($IsMoveType) { $L1_MOVE_AGE } else { $L1_MIN_AGE }
                $RemoteSubPath = if ($Sub -eq "logs") { "$SiteName/$ProjName/logs" } else { "$SiteName/$ProjName/L1/$Sub" }
                $BackupDir     = "${REMOTE_NAME}:$RemoteSubPath`_backup"

                $l1Specific = @{
                    "min-age"               = $Age
                    "backup-dir"            = $BackupDir
                    "suffix"                = "_$TIMESTAMP"
                    "suffix-keep-extension" = $true
                }

                if ($DryRun) {
                    $fullArgs = "$(Get-ArgumentsString $CommonArgs) $(Get-ArgumentsString $l1Specific)"
                    Write-Host "[SIMULATE] L1 ${Action}: $RCLONE_EXE $Action `"$FullLocalPath`" `"${REMOTE_NAME}:$RemoteSubPath`" $fullArgs" -ForegroundColor Gray
                } else {
                    & $RCLONE_EXE $Action "$FullLocalPath" "${REMOTE_NAME}:$RemoteSubPath" @CommonArgs @l1Specific
                }
            }
        }

        # 3. HANDLE LOCAL PURGE
        $PurgeDate = (Get-Date).AddMonths(-$PURGE_MONTH_AGE)
        $L1_Root = Join-Path $BASE_LOCAL "$SiteName\$ProjName\L1"
        if (Test-Path $L1_Root) {
            $OldFiles = Get-ChildItem -Path $L1_Root -Recurse -File | Where-Object { $_.LastWriteTime -lt $PurgeDate }
            foreach ($File in $OldFiles) {
                if ($DryRun) { Write-Host "[SIMULATE] Delete Old File: $($File.FullName) (Modified: $($File.LastWriteTime))" -ForegroundColor DarkRed } 
                else { if ($DebugLogging) { Write-Host "Deleting $($File.Name)" }; Remove-Item $File.FullName -Force }
            }
        }
    }
}
