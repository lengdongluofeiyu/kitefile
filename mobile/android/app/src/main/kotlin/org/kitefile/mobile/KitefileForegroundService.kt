package org.kitefile.mobile

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat

/**
 * 传输中前台保活服务（修复方案 A3.7 / Android 存活）。
 *
 * 作用：文件传输期间把进程提为前台服务，显著降低系统在锁屏/后台时
 * 回收进程的概率——本 App 的 Rust daemon 是**进程内**的，进程被杀
 * 等于传输中断。仅在「确有进行中的传输」时由 Dart 侧启动/停止，
 * 平时无通知、无常驻开销。
 *
 * 通知渠道 dataSync：Android 8.0+ 必须先建渠道；Android 14+（API 34）
 * 要求 startForeground 显式声明类型，与 manifest 的
 * `foregroundServiceType="dataSync"` 保持一致。
 */
class KitefileForegroundService : Service() {

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        createChannel()
        val notification: Notification = NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("KiteFile 正在传输")
            .setContentText("文件传输进行中，保持后台接收/发送不被系统中断")
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setOngoing(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()

        // API 29+ 起前台服务必须申报类型；API 34+ 不申报直接抛异常
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            startForeground(
                NOTIF_ID,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
            )
        } else {
            startForeground(NOTIF_ID, notification)
        }
        // 不 STICKY：被杀后不自动拉起——Dart 侧会在下次进度帧重新启动，
        // 避免出现「没有传输却常驻前台」的幽灵服务
        return START_NOT_STICKY
    }

    private fun createChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val manager = getSystemService(NotificationManager::class.java)
            val channel = NotificationChannel(
                CHANNEL_ID,
                "文件传输",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "传输进行中的前台保活通知"
            }
            manager.createNotificationChannel(channel)
        }
    }

    companion object {
        private const val CHANNEL_ID = "kitefile_transfer"
        private const val NOTIF_ID = 7879
    }
}
