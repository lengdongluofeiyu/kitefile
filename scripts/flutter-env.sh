#!/usr/bin/env bash
# Flutter / Gradle / Dart 构建环境（Git Bash）
# 与 rust-env.sh 同源：所有缓存、临时目录、产物一律落在 E 盘，不碰 C 盘。
#
# 用法： bash scripts/flutter-env.sh flutter build apk --release
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPS_ROOT="E:/zheten2.0/.deps"

# --- 临时目录：Dart frontend_server / Gradle / JVM 都会写 TEMP。
# 放 C 盘时（C 盘余量 <4G）frontend_server 会静默 exit 1，
# 表现是 "Target kernel_snapshot_program failed: Exception" 且没有任何 Dart 报错行。
export TEMP='E:\zheten2.0\.deps\tmp'
export TMP="$TEMP"
mkdir -p "$DEPS_ROOT/tmp"

# --- JDK：Gradle 9.1 兼容上限 JDK 24，取 21（JDK 25 会导致 Gradle/AGP 报错）
# 注意：PATH 里必须用 MSYS 形式（/d/...），Git Bash 不认 "D:/..." 形式的 PATH 条目。
export JAVA_HOME='D:\jdk21\jdk-21.0.12.1+1'
export PATH="/d/jdk21/jdk-21.0.12.1+1/bin:$PATH"

# --- Android SDK / Gradle / pub 缓存（这些只被 Gradle 读取，用 Windows 形式）
export ANDROID_HOME='E:\zheten2.0\.deps\android-sdk'
export ANDROID_SDK_ROOT="$ANDROID_HOME"
export GRADLE_USER_HOME='E:\zheten2.0\.deps\gradle'
export PUB_CACHE="$DEPS_ROOT/pub-cache"
export GRADLE_OPTS="${GRADLE_OPTS:-} -Dorg.gradle.daemon=false"

java.exe -version 2>&1 | head -1

cd "$REPO_ROOT/mobile"
exec "$@"
