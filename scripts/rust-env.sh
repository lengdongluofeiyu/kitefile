#!/usr/bin/env bash
# 在 MSVC 环境下执行 cargo 命令。
#
# 用法：
#   scripts/rust-env.sh cargo check --all-targets
#   scripts/rust-env.sh cargo test
#   scripts/rust-env.sh cargo build --release
#
# 为什么需要它：
#   在 Git Bash 里直接跑 cargo，链接 build script / 测试二进制时会失败，两个原因：
#     1. PATH 里排在最前的 link 是 Git 自带的 GNU `link`（/usr/bin/link），
#        不是 MSVC 的 link.exe → 报 `link: extra operand`
#     2. 缺 LIB / INCLUDE 环境变量 → 报 `LNK1181: 无法打开输入文件 "kernel32.lib"`
#   单纯 `cargo check` 有时能蒙混过关（依赖已编译好、不需要跑 linker），
#   一旦需要重新编译 build script 就会炸——所以别指望"上次能跑这次就能跑"。
#
#   只查 metadata 的命令（cargo check 且依赖齐备）其实用不到它，
#   但统一用它跑能省掉"为什么今天突然编不过"的排查。
#
# 顺带做两件事：
#   - 自动探测 VS 安装路径、MSVC 版本、Windows SDK 版本（不硬编码）
#   - 把 TEMP/TMP 指到 E 盘：C 盘空间不足时 MSVC link.exe 会静默失败

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 1. 定位 Visual Studio（vswhere 是官方支持的探测方式，VS2017+ 自带）
VSWHERE="/c/Program Files (x86)/Microsoft Visual Studio/Installer/vswhere.exe"
if [[ ! -x "$VSWHERE" ]]; then
    echo "[rust-env] 未找到 vswhere.exe，无法定位 Visual Studio" >&2
    exit 1
fi
VS_WIN="$("$VSWHERE" -latest -products '*' \
    -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 \
    -property installationPath | tr -d '\r')"
if [[ -z "$VS_WIN" ]]; then
    echo "[rust-env] vswhere 没找到装了 VC 工具链的 Visual Studio" >&2
    exit 1
fi
VS="$(cygpath -u "$VS_WIN")"

# 2. 取最新的 MSVC 工具集与 Windows SDK（可能有多个版本并存）
MSVC_VER="$(ls "$VS/VC/Tools/MSVC" 2>/dev/null | sort -V | tail -1)"
if [[ -z "$MSVC_VER" ]]; then
    echo "[rust-env] $VS 下没有 VC/Tools/MSVC" >&2
    exit 1
fi
KITS="/c/Program Files (x86)/Windows Kits/10"
SDK_VER="$(ls "$KITS/Lib" 2>/dev/null | sort -V | tail -1)"
if [[ -z "$SDK_VER" ]]; then
    echo "[rust-env] 未找到 Windows 10 SDK" >&2
    exit 1
fi

MSVC="$VS/VC/Tools/MSVC/$MSVC_VER"

# 3. 让 MSVC 的 link.exe 排在 Git 的 link 之前
export PATH="$MSVC/bin/Hostx64/x64:$PATH"

# link.exe 要的是 Windows 风格路径，且分号分隔
MSVC_W="$(cygpath -w "$MSVC")"
KITS_W='C:\Program Files (x86)\Windows Kits\10'
export LIB="$MSVC_W\\lib\\x64;$KITS_W\\Lib\\$SDK_VER\\um\\x64;$KITS_W\\Lib\\$SDK_VER\\ucrt\\x64"
export INCLUDE="$MSVC_W\\include;$KITS_W\\Include\\$SDK_VER\\um;$KITS_W\\Include\\$SDK_VER\\ucrt;$KITS_W\\Include\\$SDK_VER\\shared"

# 4. 一切"可写"的东西都放 E 盘：依赖缓存、编译产物、临时文件。
#    C 盘空间紧张时 link.exe 的失败非常隐蔽（进程被杀、退出码 1181 混在一起），
#    与其事后排查，不如一开始就不往 C 盘写。
# 注意：CARGO_HOME / RUSTUP_HOME 必须与 scripts/build-all.ps1 里设的**同一个目录**，
# 否则开发期和发布构建会各下一份依赖缓存（我就是这么多出来一份 184MB 的）。
export CARGO_HOME="E:\\zheten2.0\\.deps\\cargo"
# RUSTUP_HOME 同样必须对齐 build-all.ps1：**Android 的 target 装在 E 盘这一份里**。
# 漏了它就会回落到 C:\Users\<你>\.rustup，那里只有 x86_64-pc-windows-msvc，
# 交叉编译时报 "can't find crate for core / 考虑 rustup target add aarch64-linux-android"
# ——看起来像 target 没装，实际是找错了工具链目录。
export RUSTUP_HOME="E:\\zheten2.0\\.deps\\rustup"
# 刻意**不设** CARGO_TARGET_DIR：产物留在 core/target（同样在 E 盘，不占 C 盘），
# 与 scripts/build-all.ps1 的假设一致——脚本是从 `core\target\release\` 拷产物的。
# 各设一个等于编译两遍、白占一份几 GB 的磁盘。
mkdir -p "E:\\zheten2.0\\.deps\\tmp" 2>/dev/null || true
export TEMP="E:\\zheten2.0\\.deps\\tmp"
export TMP="$TEMP"
# 集成测试的临时目录也跟着走（storage_test / gateway_test 会读它）
export FTCORE_TEST_TMP="E:\\zheten2.0\\.deps\\tmp"

cd "$REPO_ROOT/core"
echo "[rust-env] VS=$VS_WIN  MSVC=$MSVC_VER  SDK=$SDK_VER"
exec "$@"
