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
        appendLog("[Kotlin] Service created")
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        appendLog("[Kotlin] onStartCommand: ${intent?.action}")
        when (intent?.action) {
            ACTION_START -> {
                val configJson = intent.getStringExtra("config_json") ?: run {
                    appendLog("[ERROR] No config_json"); stopSelf(); return START_NOT_STICKY
                }
                val tokenJson = intent.getStringExtra("token_json") ?: run {
                    appendLog("[ERROR] No token_json"); stopSelf(); return START_NOT_STICKY
                }
                appendLog("[Kotlin] Got config and token, starting tunnel...")
                startTunnel(configJson, tokenJson)
            }
            ACTION_STOP -> stopTunnel()
        }
        return START_NOT_STICKY
    }

    private fun startTunnel(configJson: String, tokenJson: String) {
        // ── مرحله ۱: بررسی library ───────────────────────────────────────────
        appendLog("[Kotlin] Step 1: Loading JNI library...")
        if (!FlowBridge.load()) {
            appendLog("[ERROR] Cannot load libflowdriver.so — APK must be built with GitHub Actions")
            isRunning = false
            onStatusChange?.invoke(false)
            stopSelf()
            return
        }
        appendLog("[Kotlin] Library loaded OK")

        // ── مرحله ۲: نوشتن فایل‌ها ───────────────────────────────────────────
        appendLog("[Kotlin] Step 2: Writing files...")
        val credFile  = File(filesDir, "credentials.json")
        val tokenFile = File(filesDir, "credentials.json.token")
        val debugLog  = File(filesDir, "fd_debug.log")

        if (!credFile.exists()) {
            appendLog("[ERROR] credentials.json not found in ${filesDir.absolutePath}")
            stopSelf(); return
        }
        appendLog("[Kotlin] credFile exists: ${credFile.length()} bytes")

        try {
            tokenFile.writeText(tokenJson)
            appendLog("[Kotlin] tokenFile written: ${tokenFile.length()} bytes")
        } catch (e: Exception) {
            appendLog("[ERROR] Cannot write token file: ${e.message}")
            stopSelf(); return
        }

        debugLog.delete()
        appendLog("[Kotlin] debug log cleared")

        // ── مرحله ۳: شروع tunnel ─────────────────────────────────────────────
        isRunning = true
        onStatusChange?.invoke(true)
        updateNotification("در حال اتصال...")

        // polling فایل لاگ
        scope.launch {
            var lastSize = 0L
            while (isRunning) {
                delay(400)
                try {
                    if (debugLog.exists() && debugLog.length() > lastSize) {
                        val newContent = debugLog.readText()
                        val newLines = newContent.lines()
                        val alreadyShown = if (lastSize == 0L) 0
                            else newContent.substring(0, lastSize.toInt().coerceAtMost(newContent.length - 1))
                                .count { it == '\n' }
                        newLines.drop(alreadyShown).forEach { line ->
                            if (line.isNotBlank()) appendLog("[Go] $line")
                        }
                        lastSize = debugLog.length()
                    }
                } catch (_: Exception) {}
            }
            // بعد از اتمام، بقیه لاگ رو بخون
            delay(600)
            try {
                if (debugLog.exists()) {
                    debugLog.readText().lines().forEach { line ->
                        if (line.isNotBlank()) appendLog("[Go-final] $line")
                    }
                }
            } catch (_: Exception) {}
        }

        jniExecutor.submit {
            appendLog("[Kotlin] Step 3: Calling JNI startTunnel...")
            appendLog("[Kotlin] credFile: ${credFile.absolutePath}")
            appendLog("[Kotlin] tokenFile: ${tokenFile.absolutePath}")

            try {
                val result = FlowBridge.startTunnel(
                    configJson,
                    credFile.absolutePath,
                    tokenFile.absolutePath
                )
                Thread.sleep(800)
                appendLog("[Kotlin] startTunnel returned: $result")
                if (result == -2) {
                    appendLog("[ERROR] Login failed (code -2)")
                }
            } catch (e: UnsatisfiedLinkError) {
                appendLog("[ERROR] UnsatisfiedLinkError: ${e.message}")
                Log.e("FlowService", "UnsatisfiedLinkError", e)
            } catch (e: Exception) {
                appendLog("[ERROR] Exception in JNI: ${e.javaClass.name}: ${e.message}")
                Log.e("FlowService", "JNI exception", e)
            } catch (e: Error) {
                appendLog("[ERROR] Error in JNI: ${e.javaClass.name}: ${e.message}")
                Log.e("FlowService", "JNI error", e)
            } finally {
                appendLog("[Kotlin] JNI thread finished")
                isRunning = false
                onStatusChange?.invoke(false)
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }

        scope.launch {
            delay(7000)
            if (isRunning) {
                appendLog("[INFO] ✓ SOCKS5 روی 127.0.0.1:1080 فعال")
                updateNotification("✓ متصل")
            }
        }
    }

    private fun stopTunnel() {
        appendLog("[Kotlin] Stopping tunnel...")
        try { FlowBridge.flowStop() } catch (e: Exception) {
            appendLog("[WARN] flowStop error: ${e.message}")
        }
        isRunning = false
        onStatusChange?.invoke(false)
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun appendLog(line: String) {
        val ts = java.text.SimpleDateFormat("HH:mm:ss.SSS", java.util.Locale.getDefault())
            .format(java.util.Date())
        val entry = "[$ts] $line"
        synchronized(logLines) {
            logLines.add(entry)
            if (logLines.size > 500) logLines.removeAt(0)
        }
        Log.d("FlowDriver", entry)
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
        appendLog("[Kotlin] Service destroyed")
        try { FlowBridge.flowStop() } catch (_: Exception) {}
        scope.cancel()
        jniExecutor.shutdown()
        isRunning = false
    }
}
