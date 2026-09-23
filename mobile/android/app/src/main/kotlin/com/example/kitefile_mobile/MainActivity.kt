package com.example.kitefile_mobile

import android.content.ActivityNotFoundException
import android.content.Intent
import android.database.Cursor
import android.net.Uri
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
                    // 用系统默认应用打开接收到的文件（FileProvider 授权）
                    "openFile" -> {
                        val path = call.arguments as? String
                        if (path.isNullOrEmpty()) {
                            result.error("BAD_ARGS", "path required", null)
                            return@setMethodCallHandler
                        }
                        openFile(path, result)
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
                ))
            }
            result.success(picks)
        } catch (e: Exception) {
            result.error("PICK_FAILED", e.message, null)
        }
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
}
