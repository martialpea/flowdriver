package com.flowdriver.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import com.flowdriver.ui.MainActivity
import kotlinx.coroutines.*
import java.io.File
import java.util.concurrent.Executors

class FlowService : Service() {

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val jniExecutor = Executors.newSingleThreadExecutor()

    companion object {
        const val ACTION_START = "com.flowdriver.START"
        const val ACTION_STOP  = "com.flowdriver.STOP"
        const val CHANNEL_ID   = "flowdriver_channel"
        const val NOTIF_ID     = 1
        var isRunning  = false
        var logLines   = mutableListOf<String>()
        var onLogUpdate:    ((String) -> Unit)? = null
        var onStatusChange: ((Boolean) -> Unit)? = null
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
        startForeground(NOTIF_ID, buildNotification("آماده..."))
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> {
                val configJson = intent.getStringExtra("config_json") ?: run {
                    appendLog("[ERROR] No config"); stopSelf(); return START_NOT_STICKY
                }
                val tokenJson = intent.getStringExtra("token_json") ?: run {
                    appendLog("[ERROR] No token"); stopSelf(); return START_NOT_STICKY
                }
                startTunnel(configJson, tokenJson)
            }
            ACTION_STOP -> stopTunnel()
        }
        return START_NOT_STICKY
    }

    private fun startTunnel(configJson: String, tokenJson: String) {
        if (!FlowBridge.load()) {
            appendLog("[ERROR] Cannot load libflowdriver.so")
            stopSelf(); return
        }

        val credFile  = File(filesDir, "credentials.json")
        val tokenFile = File(filesDir, "credentials.json.token")
        // FIX: لاگ فایل در همون دایرکتوری — Go اینجا می‌نویسه
        val debugLog  = File(filesDir, "fd_debug.log")

        if (!credFile.exists()) {
            appendLog("[ERROR] credentials.json پیدا نشد")
            stopSelf(); return
        }

        tokenFile.writeText(tokenJson)
        // پاک کردن لاگ قبلی
        debugLog.delete()

        appendLog("[INFO] Starting JNI tunnel...")
        appendLog("[INFO] Log file: ${debugLog.absolutePath}")

        isRunning = true
        onStatusChange?.invoke(true)
        updateNotification("در حال اتصال...")

        // polling فایل لاگ هر ۵۰۰ms
        scope.launch {
            var lastSize = 0L
            while (isRunning) {
                delay(500)
                try {
                    if (debugLog.exists()) {
                        val currentSize = debugLog.length()
                        if (currentSize > lastSize) {
                            val content = debugLog.readText()
                            val lines = content.lines()
                            val newLines = if (lastSize == 0L) {
                                lines
                            } else {
                                // فقط خطوط جدید
                                val prevLines = content.substring(0, lastSize.toInt().coerceAtMost(content.length)).lines().size
                                lines.drop(prevLines.coerceAtLeast(0))
                            }
                            newLines.forEach { line ->
                                if (line.isNotBlank()) appendLog(line)
                            }
                            lastSize = currentSize
                        }
                    }
                } catch (_: Exception) {}
            }

            // خواندن آخرین لاگ بعد از اتمام
            delay(500)
            try {
                if (debugLog.exists()) {
                    val content = debugLog.readText()
                    appendLog("=== Final Debug Log ===")
                    content.lines().forEach { line ->
                        if (line.isNotBlank()) appendLog(line)
                    }
                }
            } catch (_: Exception) {}
        }

        jniExecutor.submit {
            try {
                val result = FlowBridge.startTunnel(
                    configJson,
                    credFile.absolutePath,
                    tokenFile.absolutePath
                )
                // صبر می‌کنیم تا polling آخرین لاگ رو بخونه
                Thread.sleep(1000)
                appendLog("[INFO] Tunnel result: $result")
            } catch (e: Exception) {
                appendLog("[ERROR] JNI: ${e.message}")
                Log.e("FlowService", "JNI error", e)
            } finally {
                isRunning = false
                onStatusChange?.invoke(false)
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }

        scope.launch {
            delay(6000)
            if (isRunning) {
                appendLog("[INFO] ✓ SOCKS5 فعال روی 127.0.0.1:1080")
                updateNotification("✓ متصل")
            }
        }
    }

    private fun stopTunnel() {
        try { FlowBridge.flowStop() } catch (_: Exception) {}
        isRunning = false
        onStatusChange?.invoke(false)
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun appendLog(line: String) {
        val ts = java.text.SimpleDateFormat("HH:mm:ss", java.util.Locale.getDefault())
            .format(java.util.Date())
        val entry = "[$ts] $line"
        synchronized(logLines) {
            logLines.add(entry)
            if (logLines.size > 500) logLines.removeAt(0)
        }
        onLogUpdate?.invoke(entry)
    }

    private fun createNotificationChannel() {
        val ch = NotificationChannel(CHANNEL_ID, "FlowDriver", NotificationManager.IMPORTANCE_LOW)
        getSystemService(NotificationManager::class.java).createNotificationChannel(ch)
    }

    private fun buildNotification(text: String): Notification {
        val pi = PendingIntent.getActivity(
            this, 0, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE
        )
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("FlowDriver")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .setContentIntent(pi)
            .setOngoing(true)
            .build()
    }

    private fun updateNotification(text: String) {
        getSystemService(NotificationManager::class.java).notify(NOTIF_ID, buildNotification(text))
    }

    override fun onDestroy() {
        super.onDestroy()
        try { FlowBridge.flowStop() } catch (_: Exception) {}
        scope.cancel()
        jniExecutor.shutdown()
        isRunning = false
    }
}
