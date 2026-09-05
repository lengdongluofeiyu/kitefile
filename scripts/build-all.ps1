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
#
# 构建前会跑前置检查（cargo test / flutter analyze），任一项失败即 exit 1 且不产出 dist，
# 避免编译不过的代码被打包进发布产物。用 -SkipRust / -SkipWindows / -SkipAndroid
# 可同时跳过对应的检查与构建环节。

[CmdletBinding()]
param(
    [switch]$SkipRust,
    [switch]$SkipWindows,
    [switch]$SkipAndroid,
    [switch]$Clean,
    [switch]$VerboseLog
)

$ErrorActionPreference = 'Stop'

# ===== BOM 自愈 =====
# 本文件已两次出现「重复 BOM」（开头叠了两个 EF BB BF），会让部分工具把首行解析错。
# 根因**尚未定位**——已实测排除两种常见嫌疑：
#   1) PowerShell 5.1 的 Set-Content / Out-File（Get-Content 会识别并剥离原 BOM，
#      写出来仍是单个 BOM，不会叠加）
#   2) 常规编辑器写入（同样的单 BOM 结果）
# 所以这里不做阻断式报错（那只会每次卡住构建、逼人手动修），
# 而是自动修掉并留痕：构建照常进行，问题记在警告里，将来排查有线索。
$BomBytes = [byte[]](0xEF, 0xBB, 0xBF)
$SelfPath = $MyInvocation.MyCommand.Path
if ($SelfPath -and (Test-Path $SelfPath)) {
    $AllBytes = [System.IO.File]::ReadAllBytes($SelfPath)
    $BomCount = 0
    while ((($BomCount + 1) * 3) -le $AllBytes.Length) {
        $IsBom = $true
        for ($i = 0; $i -lt 3; $i++) {
            if ($AllBytes[($BomCount * 3) + $i] -ne $BomBytes[$i]) { $IsBom = $false; break }
        }
        if (-not $IsBom) { break }
        $BomCount++
    }
    if ($BomCount -eq 0) {
        Write-Host '[BOM] 警告：脚本缺少 BOM，中文注释可能被 PowerShell 5.1 按 ANSI 解码而乱码' -ForegroundColor Yellow
    }
    elseif ($BomCount -gt 1) {
        Write-Host "[BOM] 检测到 $BomCount 个重复 BOM，已自动修复为 1 个" -ForegroundColor Yellow
        Write-Host '[BOM] 来源未知（已排除 Set-Content 与常规编辑器写入）；若反复出现请记录触发操作' -ForegroundColor Yellow
        $Keep = New-Object byte[] ($AllBytes.Length - (($BomCount - 1) * 3))
        [Array]::Copy($AllBytes, ($BomCount - 1) * 3, $Keep, 0, $Keep.Length)
        [System.IO.File]::WriteAllBytes($SelfPath, $Keep)
    }
}

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
# TEMP/TMP 必须一起重定向：rustc 与 MSVC link.exe 默认把临时文件、.pdb 写进 %TEMP%
# （用户目录下，C 盘）。C 盘写满时的症状是 rustc ICE（encode_metadata 里 expect 失败）
# 加 LNK1201（写 pdb 失败），看起来像编译器 bug，实际是磁盘空间不足。
$env:TEMP              = 'E:\zheten2.0\.deps\tmp'
$env:TMP               = $env:TEMP
$env:PATH              = "$env:CARGO_HOME\bin;$env:JAVA_HOME\bin;$env:ANDROID_HOME\cmdline-tools\latest\bin;$env:ANDROID_HOME\platform-tools;$env:PATH"

# 临时目录不存在就建一个（首次在新机器上跑时）
if (-not (Test-Path $env:TEMP)) {
    New-Item -ItemType Directory -Path $env:TEMP -Force | Out-Null
}

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

# ===== 0.5 构建前置检查（闸门）=====
# 位置必须在 Stop-FtcoreProcesses 之前，原因有二：
#   1) 测试不占用产物文件锁，不需要杀进程；
#   2) 闸门是要「拦住坏代码」，而杀进程会打断用户正在运行的 daemon——
#      若闸门失败却已经把人家的 daemon 杀了，属于无谓的副作用。
# 任一项失败即 exit 1 且不产出 dist（退出码由 Invoke-Build 内部处理）。
if (-not $SkipRust) {
    Invoke-Build '前置检查 cargo test (core)' {
        Push-Location $CoreDir
        cargo test 2>&1 | Out-Host
        Pop-Location
    }
}
if (-not $SkipWindows) {
    Invoke-Build '前置检查 flutter analyze (desktop)' {
        Push-Location $DesktopDir
        flutter pub get 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "flutter pub get 失败 (desktop)" }
        flutter analyze 2>&1 | Out-Host
        Pop-Location
    }
}
if (-not $SkipAndroid) {
    Invoke-Build '前置检查 flutter analyze (mobile)' {
        Push-Location $MobileDir
        flutter pub get 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "flutter pub get 失败 (mobile)" }
        flutter analyze 2>&1 | Out-Host
        Pop-Location
    }
}

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
