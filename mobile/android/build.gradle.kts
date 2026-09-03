allprojects {
    repositories {
        google()
        mavenCentral()
    }
}

val newBuildDir: Directory =
    rootProject.layout.buildDirectory
        .dir("../../build")
        .get()
rootProject.layout.buildDirectory.value(newBuildDir)

subprojects {
    val newSubprojectBuildDir: Directory = newBuildDir.dir(project.name)
    project.layout.buildDirectory.value(newSubprojectBuildDir)
}
// 插件子工程（file_picker 等）的 AAR 元数据要求 compileSdk ≥ 36，
// 但它们默认按 android-34 编译，AGP 的 checkReleaseAarMetadata 校验会直接失败。
// 在各模块求值完成后（afterEvaluate）把 compileSdk 覆盖为 36。
// - 必须排除 :app —— 下方 evaluationDependsOn(":app") 会立即求值 :app，
//   对已求值项目注册 afterEvaluate 会抛
//   "Cannot run Project.afterEvaluate(Action) when the project is already evaluated"
//   （第二轮失败的原因）；
// - 不能用 plugins.withId —— 插件刚应用时模块的 android{} 尚未执行，
//   extensions.findByName("android") 为 null 被静默跳过（第三轮失败的原因）；
// - 反射调用 setter 以兼容 AGP 9.x 与旧版 DSL（不静态引用 AGP 类）。
subprojects {
    if (project.name == "app") return@subprojects
    afterEvaluate {
        val ext = project.extensions.findByName("android") ?: return@afterEvaluate
        runCatching {
            ext.javaClass.methods
                .firstOrNull { it.name == "setCompileSdk" && it.parameterCount == 1 }
                ?.invoke(ext, 36)
        }
        runCatching {
            ext.javaClass.methods
                .firstOrNull {
                    it.name == "setCompileSdkVersion" &&
                        it.parameterCount == 1 &&
                        it.parameterTypes[0] == String::class.java
                }
                ?.invoke(ext, "android-36")
        }
    }
}
subprojects {
    project.evaluationDependsOn(":app")
}

tasks.register<Delete>("clean") {
    delete(rootProject.layout.buildDirectory)
}
