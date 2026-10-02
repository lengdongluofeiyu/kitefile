package org.kitefile.mobile

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.database.Cursor
import android.net.Uri
import android.net.wifi.WifiManager
import android.os.Bundle
import android.os.Environment
import android.provider.OpenableColumns
import android.provider.Settings
import android.webkit.MimeTypeMap
import androidx.core.content.FileProvider
import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.MethodChannel
import java.io.File

class MainActivity : FlutterActivity() {
    companion object {
        private const val CHANNEL = "kitefile/native"
        private const val PICK_FILES_REQUEST = 4201

        /// mDNS 组播接收锁（P2 修复：双方开了配对模式却互相看不见）。
        ///
        /// Android 只声明 `CHANGE_WIFI_MULTICAST_STATE` 权限**不够**——
        /// 不持 MulticastLock 时系统直接丢弃发给本应用的组播包（发送不受
        /// 影响），症状是「查询-单播应答的初次发现能通，unsolicited 组播
        /// 公告（如配对模式 pair=1 的 TXT 更新）永远收不到」。
        ///
        /// 锁放在 companion 常驻：静态字段不会被 GC（锁对象被回收会
        /// 隐式释放），Activity 重建也幂等。
        private var multicastLock: WifiManager.MulticastLock? = null

        fun acquireMulticastLock(context: Context) {
            if (multicastLock?.isHeld == true) return
            val wifi =
                context.applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
            val lock = wifi.createMulticastLock("kitefile-mdns")
            lock.setReferenceCounted(false)
            lock.acquire()
            multicastLock = lock
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        acquireMulticastLock(applicationContext)
    }

    /// pickFiles 的待返回结果（选择器返回后经 onActivityResult 回填）
    private var pendingPickResult: MethodChannel.Result? = null

    /// SAF 选中的文件保持 fd 打开：Rust daemon（同进程）通过
    /// /proc/self/fd/<fd> 直读原始文件，避免 file_picker 把整个文件
    /// 拷贝到缓存目录（2GB 文件会卡死 UI + 双倍占用存储）。
    /// 持有 ParcelFileDescriptor 引用直到进程结束，fd 不会失效。
    private val openPfds = HashMap<Int, android.os.ParcelFileDescriptor>()

    override fun configureFlutterEngine(flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)
        MethodChannel(flutterEngine.dartExecutor.binaryMessenger, CHANNEL)
            .setMethodCallHandler { call, result ->
                when (call.method) {
                    // 应用专属外部存储目录（无需存储权限，其他 app 可通过文件管理器访问）
                    "getExternalFilesDir" -> {
                        val dir = getExternalFilesDir(null)
                        if (dir != null) {
                            if (!dir.exists()) dir.mkdirs()
                            result.success(dir.absolutePath)
                        } else {
                            result.error("UNAVAILABLE", "external files dir unavailable", null)
                        }
                    }
                    // 设备型号（如 "Xiaomi 13"），做 daemon 默认设备名。
                    // 不传的话 Rust 侧回退 USERNAME 环境变量——Android 上不存在，
                    // 默认名会变成 "device-xxxx" 这种无信息量的名字。
                    "getDeviceModel" -> result.success(android.os.Build.MODEL)
                    // 传输中前台保活（A3.7）：有进行中的传输才启动/停止
                    "startTransferKeepAlive" -> {
                        try {
                            val intent = Intent(this, KitefileForegroundService::class.java)
                            if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.O) {
                                startForegroundService(intent)
                            } else {
                                startService(intent)
                            }
                            result.success(true)
                        } catch (e: Exception) {
                            // 后台启动受限等场景：保活失败不阻断传输本身
                            result.error("KEEP_ALIVE_FAILED", e.message, null)
                        }
                    }
                    "stopTransferKeepAlive" -> {
                        stopService(Intent(this, KitefileForegroundService::class.java))
                        result.success(true)
                    }
                    // 用系统默认应用打开接收到的文件（FileProvider 授权）
                    "openFile" -> {
                        val path = call.arguments as? String
                        if (path.isNullOrEmpty()) {
                            result.error("BAD_ARGS", "path required", null)
                            return@setMethodCallHandler
                        }
                        openFile(path, result)
                    }
                    // 打开文件夹（DocumentsUI / 系统文件管理器）
                    "openFolder" -> {
                        val path = call.arguments as? String
                        if (path.isNullOrEmpty()) {
                            result.error("BAD_ARGS", "path required", null)
                            return@setMethodCallHandler
                        }
                        openFolder(path, result)
                    }
                    // 系统文件选择器（ACTION_OPEN_DOCUMENT，多选）。
                    // 返回 [{name, size, path}]，path = /proc/self/fd/N 直读原始文件。
                    "pickFiles" -> {
                        pendingPickResult = result
                        try {
                            val intent = Intent(Intent.ACTION_OPEN_DOCUMENT).apply {
                                addCategory(Intent.CATEGORY_OPENABLE)
                                setType("*/*")
                                putExtra(Intent.EXTRA_ALLOW_MULTIPLE, true)
                            }
                            startActivityForResult(intent, PICK_FILES_REQUEST)
                        } catch (e: ActivityNotFoundException) {
                            pendingPickResult = null
                            result.error("NO_PICKER", "设备没有文件选择器", null)
                        }
                    }
                    // 是否已授予「所有文件访问」权限（写公共目录需要）
                    "hasManageStorage" -> result.success(Environment.isExternalStorageManager())
                    // 跳转到系统设置页申请「所有文件访问」权限
                    "requestManageStorage" -> {
                        try {
                            val intent = Intent(
                                Settings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION,
                                Uri.parse("package:$packageName")
                            )
                            startActivity(intent)
                            result.success(true)
                        } catch (e: ActivityNotFoundException) {
                            // 部分设备没有该设置页，退回应用详情页
                            try {
                                startActivity(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:$packageName")))
                                result.success(true)
                            } catch (e2: Exception) {
                                result.error("NO_SETTINGS", e2.message, null)
                            }
                        }
                    }
                    else -> result.notImplemented()
                }
            }
    }

    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        if (requestCode != PICK_FILES_REQUEST) {
            super.onActivityResult(requestCode, resultCode, data)
            return
        }
        val result = pendingPickResult ?: return
        pendingPickResult = null

        if (resultCode != RESULT_OK || data == null) {
            result.success(null) // 用户取消
            return
        }

        val uris = ArrayList<Uri>()
        data.data?.let { uris.add(it) }
        data.clipData?.let { clip ->
            for (i in 0 until clip.itemCount) uris.add(clip.getItemAt(i).uri)
        }
        if (uris.isEmpty()) {
            result.success(null)
            return
        }

        try {
            val picks = ArrayList<Map<String, Any>>(uris.size)
            for (uri in uris) {
                val name = queryDisplayName(uri) ?: "file"
                val size = querySize(uri)
                val pfd = contentResolver.openFileDescriptor(uri, "r")
                    ?: throw IllegalStateException("无法打开所选文件: $uri")
                synchronized(openPfds) { openPfds[pfd.fd] = pfd }
                picks.add(mapOf(
                    "name" to name,
                    "size" to size,
                    "path" to "/proc/self/fd/${pfd.fd}",
                    // 真实路径（尽力解析）：/proc/self/fd 是进程内临时句柄，
                    // 传完即失效——传输历史里拿它打开文件/文件夹必然失败。
                    // 能解析出真实路径时优先用真实路径读写（需已授所有文件访问）。
                    // 解析失败给空串（保持 Map<String, Any> 推断，Kotlin 不收 null 值）
                    "realPath" to (resolveRealPath(uri) ?: ""),
                ))
            }
            result.success(picks)
        } catch (e: Exception) {
            result.error("PICK_FAILED", e.message, null)
        }
    }

    /// 尽力从 SAF Uri 解析真实文件路径（2026-10-02）：
    /// - externalstorage.documents（系统文件选择器最常见）：docId =
    ///   "primary:DCIM/Camera/x.mp4" → /storage/emulated/0/DCIM/Camera/x.mp4
    /// - media（MediaStore）：查 DATA 列（部分系统返回真实路径）
    /// 解析不出返回 null（调用方退回 /proc/self/fd 直读）。
    private fun resolveRealPath(uri: Uri): String? {
        try {
            if (uri.authority == "com.android.externalstorage.documents") {
                val docId = uri.lastPathSegment ?: return null
                val sep = docId.indexOf(':')
                if (sep > 0) {
                    val volume = docId.substring(0, sep)
                    val rel = docId.substring(sep + 1)
                    if (volume == "primary") {
                        val abs = "/storage/emulated/0/$rel"
                        // 必须可读才返回真实路径：否则引擎按真实路径读会失败，
                        // 退回 /proc/self/fd 直读（fd 一定可读）
                        if (File(abs).exists() && File(abs).canRead()) return abs
                    }
                }
                return null
            }
            if (uri.authority?.startsWith("com.android.providers.media") == true ||
                uri.authority?.startsWith("media") == true
            ) {
                contentResolver.query(
                    uri, arrayOf(android.provider.MediaStore.MediaColumns.DATA),
                    null, null, null,
                )?.use { c ->
                    if (c.moveToFirst()) {
                        val idx = c.getColumnIndex(android.provider.MediaStore.MediaColumns.DATA)
                        if (idx >= 0) {
                        val p = c.getString(idx)
                        if (!p.isNullOrEmpty() && File(p).canRead()) return p
                        }
                    }
                }
            }
        } catch (_: Exception) {
            // 尽力而为：解析失败不阻断选择流程
        }
        return null
    }

    /// 查询 SAF 文件的显示名（无法查询时返回 null）
    private fun queryDisplayName(uri: Uri): String? =
        contentResolver.query(uri, arrayOf(OpenableColumns.DISPLAY_NAME), null, null, null)
            ?.use { c: Cursor ->
                val idx = c.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                if (idx >= 0 && c.moveToFirst()) c.getString(idx) else null
            }

    /// 查询 SAF 文件大小（无法查询时返回 0L）
    private fun querySize(uri: Uri): Long =
        contentResolver.query(uri, arrayOf(OpenableColumns.SIZE), null, null, null)
            ?.use { c: Cursor ->
                val idx = c.getColumnIndex(OpenableColumns.SIZE)
                if (idx >= 0 && c.moveToFirst() && !c.isNull(idx)) c.getLong(idx) else 0L
            } ?: 0L

    private fun openFile(path: String, result: MethodChannel.Result) {
        try {
            val file = File(path)
            if (!file.exists()) {
                result.error("NOT_FOUND", "file not found: $path", null)
                return
            }
            val uri = FileProvider.getUriForFile(this, "${packageName}.fileprovider", file)
            val mime = MimeTypeMap.getSingleton()
                .getMimeTypeFromExtension(file.extension) ?: "application/octet-stream"
            val intent = Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(uri, mime)
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }
            startActivity(intent)
            result.success(true)
        } catch (e: ActivityNotFoundException) {
            result.error("NO_APP", "没有应用可以打开此文件类型", null)
        } catch (e: Exception) {
            result.error("OPEN_FAILED", e.message, null)
        }
    }

    /// 打开目录：DocumentsUI → FileProvider 目录 → 常见文件管理器
    private fun openFolder(path: String, result: MethodChannel.Result) {
        try {
            val file = File(path)
            val dir = if (file.isDirectory) file else file.parentFile
            if (dir == null || !dir.isDirectory) {
                result.error("NOT_FOUND", "directory not found: $path", null)
                return
            }
            val abs = dir.absolutePath
            val docId = when {
                abs.startsWith("/storage/emulated/0/") ->
                    "primary:" + abs.removePrefix("/storage/emulated/0/")
                abs == "/storage/emulated/0" -> "primary:"
                else -> abs
            }

            // 1) 系统文件管理器（显式包名）+ EXTRA_INITIAL_URI 直达目录：
            //    不带包名的 ACTION_VIEW 会被第三方 App 抢占（真机：解压专家
            //    cache 里的 0 字节假文件）；只给文档 URI 不带 INITIAL_URI 时
            //    部分 ROM 只开根页不跳转（用户要求按保存路径直达）。
            //    INITIAL_URI 用字符串常量（符号常量 API 26+ 才有，minSdk 24）。
            val documentsUri = Uri.parse(
                "content://com.android.externalstorage.documents/document/$docId"
            )
            for (pkg in listOf(
                "com.android.documentsui",
                "com.google.android.documentsui",
            )) {
                try {
                    val intent = Intent(Intent.ACTION_VIEW).apply {
                        setPackage(pkg)
                        addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                        addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                        data = documentsUri
                        putExtra("android.provider.extra.INITIAL_URI", documentsUri)
                    }
                    startActivity(intent)
                    result.success(true)
                    return
                } catch (_: Exception) {
                }
            }

            // 2) 厂商文件管理器的目录深链：folder://<path> + resource/folder
            //    （小米/OPPO 等自带管理器支持该约定；不用 file:// —— API 24+
            //    默认 StrictMode 会抛 FileUriExposedException）
            try {
                val intent = Intent(Intent.ACTION_VIEW).apply {
                    addCategory(Intent.CATEGORY_DEFAULT)
                    setDataAndType(Uri.parse("folder://$abs"), "resource/folder")
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                startActivity(intent)
                result.success(true)
                return
            } catch (_: Exception) {
            }

            // 3) 小米文件管理器按类名直开（folder:// 约定的类名变体）
            try {
                val intent = Intent(Intent.ACTION_VIEW).apply {
                    setClassName(
                        "com.android.fileexplorer",
                        "com.android.fileexplorer.activity.FileExplorerActivity",
                    )
                    setDataAndType(Uri.parse("folder://$abs"), "resource/folder")
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                startActivity(intent)
                result.success(true)
                return
            } catch (_: Exception) {
            }

            // 4) 不限包名的 ACTION_VIEW（最后手段：可能被第三方接管，
            //    但总比打不开强——DocumentsUI 缺失时的兜底）
            try {
                val intent = Intent(Intent.ACTION_VIEW).apply {
                    addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                    data = documentsUri
                }
                startActivity(intent)
                result.success(true)
                return
            } catch (_: Exception) {
            }

            // 5) FileProvider 目录 URI
            try {
                val uri = FileProvider.getUriForFile(this, "${packageName}.fileprovider", dir)
                val intent = Intent(Intent.ACTION_VIEW).apply {
                    addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                    data = uri
                }
                startActivity(intent)
                result.success(true)
                return
            } catch (_: Exception) {
            }

            // 6) 常见文件管理器（兜底兜底）：拉起根页，用户按路径自行进入
            val managers = listOf(
                "com.android.fileexplorer",
                "com.coloros.filemanager",
                "com.heytap.filemanager",
                "com.estrongs.android.pop",
            )
            for (pkg in managers) {
                try {
                    val pmIntent = packageManager.getLaunchIntentForPackage(pkg)
                    if (pmIntent != null) {
                        pmIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                        startActivity(pmIntent)
                        result.success(true)
                        return
                    }
                } catch (_: Exception) {
                }
            }
            result.error("NO_APP", "未找到可用的文件管理器: $dir", null)
        } catch (e: Exception) {
            result.error("OPEN_FOLDER_FAILED", e.message, null)
        }
    }
}
