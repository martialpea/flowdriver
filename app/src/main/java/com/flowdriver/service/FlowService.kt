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

class FlowService : Service() {

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

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
        startForeground(NOTIF_ID, buildNotification("آماده اتصال..."))
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
        scope.launch {
            try {
                // بارگذاری library — اگه fail شد crash نمی‌کنه
                if (!FlowBridge.load()) {
                    appendLog("[ERROR] Cannot load libflowdriver.so")
                    appendLog("[HINT]  APK باید از GitHub Actions build شده باشه")
                    finishWithError()
                    return@launch
                }

                // ذخیره token در فایل
                val tokenFile = File(filesDir, "credentials.json.token")
                tokenFile.writeText(tokenJson)

                // credentials.json از assets
                val credFile = File(filesDir, "credentials.json")
                if (!credFile.exists()) {
                    try {
                        assets.open("credentials.json").use { it.copyTo(credFile.outputStream()) }
                    } catch (_: Exception) {}
                }

                val credPath = if (credFile.exists()) credFile.absolutePath else tokenFile.absolutePath

                appendLog("[INFO] Starting JNI tunnel...")
                updateNotification("در حال اتصال...")
                isRunning = true
                onStatusChange?.invoke(true)

                // صدا زدن Go از JNI
                val result = FlowBridge.startTunnel(configJson, tokenJson, credPath)
                if (result != 0) {
                    appendLog("[ERROR] startTunnel returned $result")
                    finishWithError()
                    return@launch
                }

                appendLog("[INFO] Tunnel started — SOCKS5 on 127.0.0.1:1080")
                updateNotification("✓ متصل")

                // polling تا stop
                while (isRunning && FlowBridge.flowIsRunning() == 1) {
                    delay(1000)
                }

            } catch (e: UnsatisfiedLinkError) {
                appendLog("[ERROR] UnsatisfiedLinkError: ${e.message}")
                appendLog("[HINT]  libflowdriver.so پیدا نشد در APK")
            } catch (e: Exception) {
                appendLog("[ERROR] ${e.javaClass.simpleName}: ${e.message}")
                Log.e("FlowService", "tunnel error", e)
            } finally {
                isRunning = false
                onStatusChange?.invoke(false)
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }
    }

    private fun stopTunnel() {
        appendLog("[INFO] Stopping...")
        try { FlowBridge.flowStop() } catch (e: Exception) {
            Log.e("FlowService", "flowStop error", e)
        }
        isRunning = false
        onStatusChange?.invoke(false)
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun finishWithError() {
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
            if (logLines.size > 300) logLines.removeAt(0)
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
        getSystemService(NotificationManager::class.java)
            .notify(NOTIF_ID, buildNotification(text))
    }

    override fun onDestroy() {
        super.onDestroy()
        try { FlowBridge.flowStop() } catch (_: Exception) {}
        scope.cancel()
        isRunning = false
    }
}
