# --- GLOBAL CONFIGURATION ---
$RCLONE_EXE   = "E:\Projects\rclone\1.73.0\rclone.exe"
$BASE_LOCAL   = "E:\Data"
$REMOTE_NAME  = "mssp_czodata"
$TIMESTAMP    = Get-Date -Format "yyMMdd-HHmm"

# --- LOGGING CONFIGURATION ---
$LogDir = Join-Path $PSScriptRoot "Logs"
if (!(Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$LogFile = Join-Path $LogDir "SyncLog_$TIMESTAMP.txt"

# --- TIMER INITIALIZATION ---
$Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# Helper function to print to screen AND write to file
function Write-Log {
    param([string]$Message, [string]$Color = "White")
    $LogEntry = "[$($TIMESTAMP)] $Message"
    Write-Host $Message -ForegroundColor $Color
    $LogEntry | Out-File -FilePath $LogFile -Append
}

# --- TEST SETTINGS ---
$DryRun       = $false  
$DebugLogging = $true  

# --- PARAMETERS ---
$L1_MIN_AGE      = "15m"      
$L1_MOVE_AGE     = "25h"     
$PURGE_MONTH_AGE = 3        

# --- PROJECT STRUCTURE DEFINITION ---
$Sites = @{
    "Bahada" = @{ 
	"Projects" = @( 
	    @{ Name = "CR3000"; L1Folders = @("EddyCovariance_ts", "EddyCovariance_ts_2", "Flux", "SoilSensor_CS650", "TowerClimate_met") } ) }
    "Pecan5R" = @{
        "Projects" = @(
            @{ Name = "CR6-Above"; 
	        L1Folders = @(
		    "Time_Series", "Config_Setting_Notes", "Const_Table", "CPIStatus", 
		    "Diagnostic", "Flux_AmeriFluxFormat", "Flux_Notes", "System_Operatn_Notes"
		) 
	    },
            @{ Name = "CR1000X-AddSen"; L1Folders = @("Additional_Sensors") },
            @{ Name = "CR1000X-Profile"; L1Folders = @("CalAvg", "RawData", "IntAvg", "message_log", "SiteAvg", "TimeInfo") }
        )
    }
    "RedLake" = @{ 
	"Projects" = @( @{ Name = "CR3000"; L1Folders = @("Biomet", "BiometConstants", "SoilSensor_CS650", "VariableChecks") } ) }
}

# --- ENGINE ---

if ($DryRun) { Write-Log "--- DRY RUN ENABLED: NO ACTIONS WILL BE TAKEN ---" "Yellow" }
Write-Log "Starting Sync Process. Logging to: $LogFile" "Green"

foreach ($SiteName in $Sites.Keys) {
    foreach ($Proj in $Sites[$SiteName].Projects) {
        $ProjName = $Proj.Name
        Write-Log "`n[SITE: $SiteName | PROJ: $ProjName]" "Cyan"

        # 1. HANDLE L0 DATA (MOVE)
        $L0_Source = Join-Path $BASE_LOCAL "$SiteName\$ProjName\L0"
        if (Test-Path $L0_Source) {
            $L0_Args = @(
	        "move", "$L0_Source", "${REMOTE_NAME}:$SiteName/$ProjName/L0", 
		"--update", 
		"--retries", "10", 
		"--low-level-retries", "20", 
		"--checkers", "4", 
		"--transfers", "4", 
		"--tpslimit", "8", 
		"--tpslimit-burst", "8"
	    )
            
            if ($DryRun) { 
                Write-Log "[SIMULATE] L0 Move: $RCLONE_EXE $($L0_Args -join ' ')" "Gray" 
            } 
            else { 
                Write-Log "Executing L0 Move for $ProjName..." "Gray"
                & $RCLONE_EXE $L0_Args 2>&1 | Out-File -FilePath $LogFile -Append
            }
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

                $L1_Args = @(
		    $Action, "$FullLocalPath", "${REMOTE_NAME}:$RemoteSubPath", 
		    "--update", 
		    "--retries", "10", 
		    "--low-level-retries", "20", 
		    "--checkers", "2", 
		    "--transfers", "2", 
		    "--tpslimit", "4", 
		    "--tpslimit-burst", "4", 
		    "--min-age", "$Age", 
		    "--backup-dir", "$BackupDir", 
		    "--suffix", "_$TIMESTAMP", 
		    "--suffix-keep-extension")

                if ($DryRun) {
                    Write-Log "[SIMULATE] L1 ${Action}: $RCLONE_EXE $($L1_Args -join ' ')" "Gray"
                } else {
                    Write-Log "Executing L1 $Action for $Sub..." "Gray"
                    & $RCLONE_EXE $L1_Args 2>&1 | Out-File -FilePath $LogFile -Append
                }
            }
        }

        # 3. HANDLE LOCAL PURGE
        $PurgeDate = (Get-Date).AddMonths(-$PURGE_MONTH_AGE)
        $L1_Root = Join-Path $BASE_LOCAL "$SiteName\$ProjName\L1"
        if (Test-Path $L1_Root) {
            $OldFiles = Get-ChildItem -Path $L1_Root -Recurse -File | Where-Object { $_.LastWriteTime -lt $PurgeDate }
            foreach ($File in $OldFiles) {
                if ($DryRun) { 
                    Write-Log "[SIMULATE] Delete Old File: $($File.FullName)" "DarkRed" 
                } 
                else { 
                    Write-Log "PURGE: Deleting $($File.FullName) (Last Modified: $($File.LastWriteTime))" "DarkYellow"
                    Remove-Item $File.FullName -Force 
                }
            }
        }
    }
}
# --- FINAL REPORT ---
$Stopwatch.Stop()
$ElapsedTime = $Stopwatch.Elapsed
$DurationString = "$($ElapsedTime.Hours)h $($ElapsedTime.Minutes)m $($ElapsedTime.Seconds)s"

Write-Log ("`n" + ("=" * 40)) "Green" 
Write-Log "TOTAL TIME TAKEN: $DurationString" "Green"
Write-Log "Process Finished at $(Get-Date)" "Green"
Write-Log ("=" * 40) "Green"
