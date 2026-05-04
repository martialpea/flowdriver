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
        if (!FlowBridge.load()) {
            appendLog("[ERROR] Cannot load libflowdriver.so")
            stopSelf(); return
        }

        // FIX: ساختار فایل‌ها باید دقیقاً این باشه:
        // filesDir/credentials.json       ← client_id و client_secret
        // filesDir/credentials.json.token ← refresh_token
        // Go به credFilePath = credentials.json نگاه می‌کنه
        // و .token رو از credFilePath+".token" می‌خونه

        val credFile  = File(filesDir, "credentials.json")
        val tokenFile = File(filesDir, "credentials.json.token")

        // ۱. نوشتن token
        tokenFile.writeText(tokenJson)
        appendLog("[INFO] Token written: ${tokenFile.absolutePath}")

        // ۲. credentials.json از assets
        if (!credFile.exists()) {
            try {
                assets.open("credentials.json").use { it.copyTo(credFile.outputStream()) }
                appendLog("[INFO] credentials.json loaded from assets")
            } catch (_: Exception) {
                // اگه credentials.json در assets نبود، یه dummy بساز
                // چون Go فقط client_id رو از اون می‌خونه و token داریم
                credFile.writeText("""{"installed":{"client_id":"","client_secret":"","token_uri":"https://oauth2.googleapis.com/token","redirect_uris":["http://localhost"]}}""")
                appendLog("[WARN] credentials.json not in assets, using dummy")
            }
        }

        appendLog("[INFO] credFile: ${credFile.absolutePath}")
        appendLog("[INFO] tokenFile exists: ${tokenFile.exists()}")

        isRunning = true
        onStatusChange?.invoke(true)
        updateNotification("در حال اتصال...")

        // blocking JNI در thread جداگانه
        jniExecutor.submit {
            try {
                appendLog("[INFO] Calling JNI startTunnel...")
                // فقط credFile پاس می‌کنیم — Go خودش .token رو می‌خونه
                val result = FlowBridge.startTunnel(configJson, credFile.absolutePath)
                appendLog("[INFO] Tunnel ended: $result")
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

        // نمایش وضعیت بعد از ۳ ثانیه
        scope.launch {
            delay(3000)
            if (isRunning) {
                appendLog("[INFO] ✓ SOCKS5 فعال روی 127.0.0.1:1080")
                updateNotification("✓ متصل — SOCKS5:1080")
            }
        }
    }

    private fun stopTunnel() {
        appendLog("[INFO] Stopping...")
        try { FlowBridge.flowStop() } catch (e: Exception) {
            Log.e("FlowService", "stop error", e)
        }
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
        jniExecutor.shutdown()
        isRunning = false
    }
}
