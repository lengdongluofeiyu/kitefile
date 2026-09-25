import java.util.Properties

plugins {
    id("com.android.application")
    // The Flutter Gradle Plugin must be applied after the Android and Kotlin Gradle plugins.
    id("dev.flutter.flutter-gradle-plugin")
}

// 正式签名（工作流 D）：android/key.properties 存在时用自定义 keystore，
// 否则回退 debug 签名（本地开发可跑）。发布前必须配置：
//   android/key.properties:
//     storeFile=/abs/path/upload-keystore.jks
//     storePassword=***
//     keyAlias=upload
//     keyPassword=***
// key.properties 与 *.jks 不得提交（见 android/.gitignore）。
val keystorePropertiesFile = rootProject.file("key.properties")
val keystoreProperties = Properties().apply {
    if (keystorePropertiesFile.exists()) {
        keystorePropertiesFile.inputStream().use { load(it) }
    }
}
val hasReleaseKeystore = keystorePropertiesFile.exists()

android {
    // 正式包名（工作流 D）：com.example.* 是模板占位，商店与深链要求稳定正式 ID。
    // 注意：applicationId 变更后旧包视为另一个应用（不继承其数据）。
    namespace = "org.kitefile.mobile"
    compileSdk = 36
    ndkVersion = flutter.ndkVersion

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    defaultConfig {
        applicationId = "org.kitefile.mobile"
        // SDK 版本钉死（工作流 D）：不跟随 flutter.* 隐式默认漂移。
        // minSdk 24：前台保活服务与 file_picker 实际下限；
        // targetSdk 36：与 compileSdk 对齐（Android 15 行为）。
        minSdk = 24
        targetSdk = 36
        versionCode = flutter.versionCode
        versionName = flutter.versionName

        // ABI 只出 arm64-v8a（工作流 D）：jniLibs 里只有 arm64 的 libkitefile.so，
        // 不锁的话 x86_64/armeabi-v7a 包能装上但 so 缺失、启动即崩。
        ndk {
            abiFilters += listOf("arm64-v8a")
        }
    }

    signingConfigs {
        if (hasReleaseKeystore) {
            create("release") {
                storeFile = file(keystoreProperties.getProperty("storeFile"))
                storePassword = keystoreProperties.getProperty("storePassword")
                keyAlias = keystoreProperties.getProperty("keyAlias")
                keyPassword = keystoreProperties.getProperty("keyPassword")
            }
        }
    }

    buildTypes {
        release {
            // 有正式 keystore 用它，否则 debug 签名兜底（开发构建）
            signingConfig = if (hasReleaseKeystore) {
                signingConfigs.getByName("release")
            } else {
                signingConfigs.getByName("debug")
            }
        }
    }
}

kotlin {
    compilerOptions {
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17
    }
}

flutter {
    source = "../.."
}
