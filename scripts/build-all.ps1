# FTCore 一键构建脚本
#
# 用法：
#   .\scripts\build-all.ps1                    # 全量构建
#   .\scripts\build-all.ps1 -SkipAndroid       # 跳过 Android APK
#   .\scripts\build-all.ps1 -Clean             # 构建前清空 dist/
#   .\scripts\build-all.ps1 -Verbose            # 输出详细日志
#
# 所有产物输出到 e:\zheten2.0\filetransfer\dist\
#
# 缓存全部走 E 盘，不占用 C 盘空间。

[CmdletBinding()]
param(
    [switch]$SkipRust,
    [switch]$SkipWindows,
    [switch]$SkipAndroid,
    [switch]$Clean,
    [switch]$VerboseLog
)

$ErrorActionPreference = 'Stop'

# ===== 路径与配置 =====
$ScriptRoot   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot  = Split-Path -Parent $ScriptRoot
$CoreDir      = Join-Path $ProjectRoot 'core'
$DesktopDir   = Join-Path $ProjectRoot 'desktop'
$MobileDir    = Join-Path $ProjectRoot 'mobile'
$DistDir      = Join-Path $ProjectRoot 'dist'
$DistWindows   = Join-Path $DistDir 'windows'
$DistAndroid   = Join-Path $DistDir 'android'

# ===== 环境变量（与开发期保持一致：全部 E 盘） =====
$env:CARGO_HOME        = 'E:\zheten2.0\.deps\cargo'
$env:RUSTUP_HOME       = 'E:\zheten2.0\.deps\rustup'
$env:PUB_CACHE         = 'E:\zheten2.0\.deps\pub-cache'
$env:JAVA_HOME         = 'C:\jdk22'
$env:ANDROID_HOME      = 'E:\zheten2.0\.deps\android-sdk'
$env:ANDROID_SDK_ROOT  = 'E:\zheten2.0\.deps\android-sdk'
$env:GRADLE_USER_HOME  = 'E:\zheten2.0\.deps\gradle'
$env:PATH              = "$env:CARGO_HOME\bin;$env:JAVA_HOME\bin;$env:ANDROID_HOME\cmdline-tools\latest\bin;$env:ANDROID_HOME\platform-tools;$env:PATH"

# ===== 工具函数 =====
function Write-Step([string]$msg) { Write-Host "`n[*] $msg" -ForegroundColor Cyan }
function Write-Ok([string]$msg)   { Write-Host "    [OK] $msg" -ForegroundColor Green }
function Write-Err([string]$msg)  { Write-Host "    [ERR] $msg" -ForegroundColor Red }

# 停止运行中的 FTCore 进程（daemon / 桌面端）。
# 产物文件被这些进程锁定会导致 Copy-Item / Remove-Item 失败；
# 桌面端下次启动时会自动重新拉起 daemon，无需担心。
function Stop-FtcoreProcesses {
    $stopped = $false
    foreach ($name in 'ftcore-cli', 'ftcore_desktop') {
        $procs = Get-Process -Name $name -ErrorAction SilentlyContinue
        if ($procs) {
            $procs | Stop-Process -Force
            $stopped = $true
            Write-Host "    已停止运行中的 $name.exe（更新产物需要）"
        }
    }
    if ($stopped) { Start-Sleep -Milliseconds 800 }
}

function Invoke-Build([string]$label, [scriptblock]$block) {
    Write-Step "$label ..."
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    # cargo / flutter 的进度走 stderr，Stop 模式会把它当 terminating error；这里临时切 Continue
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $block
        if ($LASTEXITCODE -ne 0 -and $null -ne $LASTEXITCODE) {
            $ErrorActionPreference = $prevEAP
            throw "exit code $LASTEXITCODE"
        }
        $sw.Stop()
        $ErrorActionPreference = $prevEAP
        Write-Ok "$label 完成 ($([math]::Round($sw.Elapsed.TotalSeconds,1))s)"
    } catch {
        $sw.Stop()
        $ErrorActionPreference = $prevEAP
        Write-Err "$label 失败: $_"
        exit 1
    }
}

# ===== 0. 准备 dist 目录 =====
Write-Step "准备 dist 目录: $DistDir"
if ($Clean -and (Test-Path $DistDir)) {
    Remove-Item -Recurse -Force $DistDir
    Write-Host "    已清空旧的 dist/"
}
New-Item -ItemType Directory -Force -Path $DistDir, $DistWindows, $DistAndroid | Out-Null
$buildTime = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
Write-Ok "dist 目录就绪"

# 停止运行中的 daemon / 桌面端（产物文件被锁会导致拷贝失败）
Stop-FtcoreProcesses

# ===== 1. Rust 核心 release =====
if (-not $SkipRust) {
    Invoke-Build 'Rust 核心 release' {
        Push-Location $CoreDir
        cargo build --release 2>&1 | Out-Host
        Pop-Location
    }
    # 拷贝 Rust 守护进程与 FFI 库到 dist\windows\
    Copy-Item "$CoreDir\target\release\ftcore-cli.exe" $DistWindows -Force
    Copy-Item "$CoreDir\target\release\ftcore.dll"     $DistWindows -Force
    Write-Ok "ftcore-cli.exe + ftcore.dll 已拷贝到 dist\windows\"
}

# ===== 2. Windows 桌面端 =====
if (-not $SkipWindows) {
    Invoke-Build 'Windows 桌面端 (Flutter)' {
        Push-Location $DesktopDir
        flutter build windows --release 2>&1 | Out-Host
        Pop-Location
    }
    $srcRelease = "$DesktopDir\build\windows\x64\runner\Release"
    # 清空旧的 windows 输出（保留 ftcore-cli.exe / ftcore.dll，因为下面会再拷一遍）
    Get-ChildItem $DistWindows -Force | Where-Object { $_.Name -notin @('ftcore-cli.exe','ftcore.dll') } | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    # 拷贝全部 Flutter 产物
    Copy-Item "$srcRelease\*" $DistWindows -Recurse -Force
    Write-Ok "Windows 桌面端已拷贝到 dist\windows\"
}

# ===== 3. Android APK =====
if (-not $SkipAndroid) {
    # 3a. Rust 交叉编译 Android arm64 .so（需 rustup target add aarch64-linux-android）
    Invoke-Build 'Rust 核心 Android arm64' {
        Push-Location $CoreDir
        cargo build --release --target aarch64-linux-android 2>&1 | Out-Host
        Pop-Location
    }
    $jniLibs = Join-Path $MobileDir 'android\app\src\main\jniLibs\arm64-v8a'
    New-Item -ItemType Directory -Force -Path $jniLibs | Out-Null
    Copy-Item "$CoreDir\target\aarch64-linux-android\release\libftcore.so" $jniLibs -Force
    Write-Ok "libftcore.so 已拷贝到 jniLibs\arm64-v8a\"

    # 3b. Flutter APK（会自动打包 jniLibs）
    Invoke-Build 'Android APK (Flutter)' {
        Push-Location $MobileDir
        flutter build apk --release 2>&1 | Out-Host
        Pop-Location
    }
    Copy-Item "$MobileDir\build\app\outputs\flutter-apk\app-release.apk" $DistAndroid -Force
    Write-Ok "Android APK 已拷贝到 dist\android\"
}

# ===== 4. 生成 manifest =====
$lines = @()
$lines += 'FTCore 构建清单'
$lines += '=========================================='
$lines += "构建时间: $buildTime"
$lines += "构建机器: $env:COMPUTERNAME"
$lines += "构建用户: $env:USERNAME"
$lines += ''
$lines += '产物列表:'
$lines += '---'
$lines += '[Windows 桌面端]'
$lines += '路径: dist\windows\'
$lines += '启动文件: ftcore_desktop.exe'
$lines += '辅助文件: ftcore-cli.exe (守护进程), ftcore.dll (FFI 库)'
$lines += ''
if (Test-Path "$DistWindows\ftcore_desktop.exe") {
    Get-ChildItem $DistWindows -Recurse -File | ForEach-Object {
        $rel = $_.FullName.Substring($DistWindows.Length + 1)
        $sizeKB = [math]::Round($_.Length/1KB,1)
        $lines += "  - $rel  ($sizeKB KB)"
    }
} else {
    $lines += '  (未构建)'
}
$lines += ''
$lines += '---'
$lines += '[Android 安装包]'
$lines += '路径: dist\android\'
$lines += '安装包: app-release.apk'
$lines += ''
if (Test-Path "$DistAndroid\app-release.apk") {
    $f = Get-Item "$DistAndroid\app-release.apk"
    $sizeMB = [math]::Round($f.Length/1MB,2)
    $lines += "  - app-release.apk  ($sizeMB MB)  built $($f.LastWriteTime)"
} else {
    $lines += '  (未构建)'
}
$lines += ''
$lines += '=========================================='
$lines += '运行说明:'
$lines += '1. Windows: 进入 dist\windows\，双击 ftcore_desktop.exe（守护进程会自动拉起）'
$lines += '2. Android: 用 adb install -r dist\android\app-release.apk 安装到手机'

$manifestPath = Join-Path $DistDir 'dist.manifest.txt'
$lines -join "`r`n" | Out-File -FilePath $manifestPath -Encoding utf8
Write-Ok "清单已生成: dist\dist.manifest.txt"

# ===== 5. 汇总 =====
Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host "  构建完成" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "产物目录: $DistDir"
Write-Host ""
if (Test-Path "$DistWindows\ftcore_desktop.exe") {
    $size = (Get-ChildItem $DistWindows -Recurse | Measure-Object Length -Sum).Sum
    Write-Host ("  Windows:  dist\windows\ftcore_desktop.exe  (总 {0} MB)" -f [math]::Round($size/1MB,1))
}
if (Test-Path "$DistAndroid\app-release.apk") {
    $size = (Get-Item "$DistAndroid\app-release.apk").Length
    Write-Host ("  Android:  dist\android\app-release.apk     ({0} MB)" -f [math]::Round($size/1MB,2))
}
Write-Host ""
