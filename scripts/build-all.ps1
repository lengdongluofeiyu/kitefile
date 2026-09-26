# KiteFile 一键构建脚本
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

# ===== 环境变量 =====
# 优先：进程环境 → scripts/local-env.ps1（本机、勿提交）→ 默认用户目录。
if (Test-Path (Join-Path $PSScriptRoot 'local-env.ps1')) {
    . (Join-Path $PSScriptRoot 'local-env.ps1')
    Write-Host '[env] loaded scripts/local-env.ps1' -ForegroundColor DarkGray
}
if (-not $env:CARGO_HOME) { $env:CARGO_HOME = Join-Path $env:USERPROFILE '.cargo' }
if (-not $env:PUB_CACHE)  { $env:PUB_CACHE  = Join-Path $env:LOCALAPPDATA 'Pub\Cache' }
if (-not $env:JAVA_HOME) {
    # Gradle 9.1 兼容上限 JDK 24，推荐 17–21
    Write-Host '[env] JAVA_HOME 未设置，Gradle 可能失败（需要 JDK 17–21）' -ForegroundColor Yellow
}
if (-not $env:ANDROID_HOME -and $env:ANDROID_SDK_ROOT) { $env:ANDROID_HOME = $env:ANDROID_SDK_ROOT }
if (-not $env:ANDROID_HOME) {
    Write-Host '[env] ANDROID_HOME 未设置，Android 构建将失败' -ForegroundColor Yellow
}
if ($env:ANDROID_HOME) { $env:ANDROID_SDK_ROOT = $env:ANDROID_HOME }
if (-not $env:GRADLE_USER_HOME) { $env:GRADLE_USER_HOME = Join-Path $env:USERPROFILE '.gradle' }
# TEMP：C 盘写满会让 rustc/linker/Dart frontend_server 神秘失败
if (-not $env:TEMP) { $env:TEMP = [System.IO.Path]::GetTempPath() }
if (-not $env:TMP)  { $env:TMP  = $env:TEMP }
if (-not $env:FTCORE_TEST_TMP) { $env:FTCORE_TEST_TMP = $env:TEMP }

# Flutter Windows 构建需要 %PROGRAMFILES(X86)%；精简环境里可能没有
if (-not (Test-Path env:'ProgramFiles(x86)')) {
    ${env:ProgramFiles(x86)} = 'C:\Program Files (x86)'
}

# Android NDK 链接器：若未在 core/.cargo/config.toml 写死路径，则从 SDK 探测并导出
if (-not $env:CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER -and $env:ANDROID_HOME) {
    $ndkRoot = Join-Path $env:ANDROID_HOME 'ndk'
    if (Test-Path $ndkRoot) {
        $ndkVer = Get-ChildItem $ndkRoot -Directory | Sort-Object Name -Descending | Select-Object -First 1
        if ($ndkVer) {
            $clang = Join-Path $ndkVer.FullName 'toolchains\llvm\prebuilt\windows-x86_64\bin\aarch64-linux-android24-clang.cmd'
            if (-not (Test-Path $clang)) {
                $clang = Get-ChildItem (Join-Path $ndkVer.FullName 'toolchains\llvm\prebuilt') -Recurse -Filter 'aarch64-linux-android*-clang.cmd' -ErrorAction SilentlyContinue |
                    Select-Object -First 1 -ExpandProperty FullName
            }
            if ($clang) {
                $env:CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER = $clang
                Write-Host "[env] NDK linker = $clang" -ForegroundColor DarkGray
            }
        }
    }
}

# PATH 组装：JAVA_HOME / ANDROID_HOME 存在才追加。
# 注意：PowerShell 子表达式里不能用 C 风格 \"...\" 转义（会把整段当命令名，
# JAVA_HOME 一设置就 CommandNotFound）；改用数组拼接，语义一目了然。
$pathParts = @("$env:CARGO_HOME\bin")
if ($env:JAVA_HOME) { $pathParts += "$env:JAVA_HOME\bin" }
if ($env:ANDROID_HOME) {
    $pathParts += "$env:ANDROID_HOME\cmdline-tools\latest\bin"
    $pathParts += "$env:ANDROID_HOME\platform-tools"
}
$env:PATH = ($pathParts -join ';') + ";$env:PATH"

# 临时目录不存在就建一个
if (-not (Test-Path $env:TEMP)) {
    New-Item -ItemType Directory -Path $env:TEMP -Force | Out-Null
}
if ($env:FTCORE_TEST_TMP -and -not (Test-Path $env:FTCORE_TEST_TMP)) {
    New-Item -ItemType Directory -Path $env:FTCORE_TEST_TMP -Force | Out-Null
}

# ===== 工具函数 =====
function Write-Step([string]$msg) { Write-Host "`n[*] $msg" -ForegroundColor Cyan }
function Write-Ok([string]$msg)   { Write-Host "    [OK] $msg" -ForegroundColor Green }
function Write-Err([string]$msg)  { Write-Host "    [ERR] $msg" -ForegroundColor Red }

# 停止运行中的 KiteFile 进程（daemon / 桌面端）。
# 产物文件被这些进程锁定会导致 Copy-Item / Remove-Item 失败；
# 桌面端下次启动时会自动重新拉起 daemon，无需担心。
function Stop-FtcoreProcesses {
    $stopped = $false
    # 含改名前的旧进程名（ftcore-*）：旧版残留会锁住 dist\windows\*.dll，
    # 只认新名字会让 Copy-Item 反复失败（2026-09-26 实测）。
    foreach ($name in 'kitefile-cli', 'kitefile_desktop', 'ftcore-cli', 'ftcore_desktop') {
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
    Copy-Item "$CoreDir\target\release\kitefile-cli.exe" $DistWindows -Force
    Copy-Item "$CoreDir\target\release\kitefile.dll"     $DistWindows -Force
    Write-Ok "kitefile-cli.exe + kitefile.dll 已拷贝到 dist\windows\"
}

# ===== 2. Windows 桌面端 =====
if (-not $SkipWindows) {
    Invoke-Build 'Windows 桌面端 (Flutter)' {
        Push-Location $DesktopDir
        flutter build windows --release 2>&1 | Out-Host
        Pop-Location
    }
    $srcRelease = "$DesktopDir\build\windows\x64\runner\Release"
    # 清空旧的 windows 输出（保留 kitefile-cli.exe / kitefile.dll，因为下面会再拷一遍）
    Get-ChildItem $DistWindows -Force | Where-Object { $_.Name -notin @('kitefile-cli.exe','kitefile.dll') } | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
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
    Copy-Item "$CoreDir\target\aarch64-linux-android\release\libkitefile.so" $jniLibs -Force
    Write-Ok "libkitefile.so 已拷贝到 jniLibs\arm64-v8a\"

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
$lines += 'KiteFile 构建清单'
$lines += '=========================================='
$lines += "构建时间: $buildTime"
$lines += "构建机器: $env:COMPUTERNAME"
$lines += "构建用户: $env:USERNAME"
$lines += ''
$lines += '产物列表:'
$lines += '---'
$lines += '[Windows 桌面端]'
$lines += '路径: dist\windows\'
$lines += '启动文件: kitefile_desktop.exe'
$lines += '辅助文件: kitefile-cli.exe (守护进程), kitefile.dll (FFI 库)'
$lines += ''
if (Test-Path "$DistWindows\kitefile_desktop.exe") {
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
$lines += '1. Windows: 进入 dist\windows\，双击 kitefile_desktop.exe（守护进程会自动拉起）'
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
if (Test-Path "$DistWindows\kitefile_desktop.exe") {
    $size = (Get-ChildItem $DistWindows -Recurse | Measure-Object Length -Sum).Sum
    Write-Host ("  Windows:  dist\windows\kitefile_desktop.exe  (总 {0} MB)" -f [math]::Round($size/1MB,1))
}
if (Test-Path "$DistAndroid\app-release.apk") {
    $size = (Get-Item "$DistAndroid\app-release.apk").Length
    Write-Host ("  Android:  dist\android\app-release.apk     ({0} MB)" -f [math]::Round($size/1MB,2))
}
Write-Host ""
