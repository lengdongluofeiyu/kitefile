#!/usr/bin/env bash
# Flutter / Gradle / Dart 构建环境（Git Bash）
# 用法： bash scripts/flutter-env.sh flutter build apk --release
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 可选本机覆盖（绝对路径写在这里，勿提交）
if [[ -f "$REPO_ROOT/scripts/local-env.sh" ]]; then
    # shellcheck source=/dev/null
    . "$REPO_ROOT/scripts/local-env.sh"
    echo "[flutter-env] loaded scripts/local-env.sh"
fi

# --- 临时目录：Dart frontend_server / Gradle / JVM 都会写 TEMP
export TEMP="${TEMP:-${TMPDIR:-/tmp}}"
export TMP="${TMP:-$TEMP}"
mkdir -p "$TEMP"

# --- JDK：Gradle 9.1 兼容上限 JDK 24，推荐 17–21
if [[ -n "${JAVA_HOME:-}" ]]; then
    export PATH="$(cygpath -u "$JAVA_HOME")/bin:$PATH"
fi

# --- Android SDK / Gradle / pub
export ANDROID_HOME="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
export ANDROID_SDK_ROOT="${ANDROID_SDK_ROOT:-$ANDROID_HOME}"
export GRADLE_USER_HOME="${GRADLE_USER_HOME:-$HOME/.gradle}"
export PUB_CACHE="${PUB_CACHE:-${LOCALAPPDATA:-$HOME}/Pub/Cache}"
export GRADLE_OPTS="${GRADLE_OPTS:-} -Dorg.gradle.daemon=false"

java.exe -version 2>&1 | head -1

cd "$REPO_ROOT/mobile"
exec "$@"
