package com.flowdriver.service

import android.Manifest
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Environment
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
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
        appendLog("[Kotlin] Service created — Android ${Build.VERSION.SDK_INT}")
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
                startTunnel(configJson, tokenJson)
            }
            ACTION_STOP -> stopTunnel()
        }
        return START_NOT_STICKY
    }

    private fun startTunnel(configJson: String, tokenJson: String) {
        // ۱. load library
        appendLog("[Kotlin] Loading JNI library...")
        if (!FlowBridge.load()) {
            appendLog("[ERROR] Failed to load libflowdriver.so")
            stopSelf(); return
        }
        appendLog("[Kotlin] Library loaded OK")

        // ۲. فایل‌ها
        val credFile  = File(filesDir, "credentials.json")
        val tokenFile = File(filesDir, "credentials.json.token")

        if (!credFile.exists()) {
            appendLog("[ERROR] credentials.json not found")
            stopSelf(); return
        }

        try {
            tokenFile.writeText(tokenJson)
        } catch (e: Exception) {
            appendLog("[ERROR] Cannot write token: ${e.message}")
            stopSelf(); return
        }

        appendLog("[Kotlin] credFile: ${credFile.absolutePath} (${credFile.length()} bytes)")
        appendLog("[Kotlin] tokenFile: ${tokenFile.absolutePath} (${tokenFile.length()} bytes)")

        // بررسی دسترسی Downloads
        val downloadsDir = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS)
        appendLog("[Kotlin] Downloads dir: ${downloadsDir.absolutePath}")
        appendLog("[Kotlin] Downloads writable: ${downloadsDir.canWrite()}")

        // ۳. شروع
        isRunning = true
        onStatusChange?.invoke(true)
        updateNotification("در حال اتصال...")

        // polling فایل لاگ Go
        scope.launch {
            val debugLog = File(filesDir, "fd_debug.log")
            val downloadsLog = File(downloadsDir, "flowdriver_debug.log")
            var lastSize = 0L
            var shown = mutableSetOf<Int>()

            while (isRunning) {
                delay(400)
                try {
                    // اول filesDir رو چک کن
                    val logToRead = when {
                        debugLog.exists() && debugLog.length() > 0 -> debugLog
                        downloadsLog.exists() && downloadsLog.length() > 0 -> downloadsLog
                        else -> null
                    }

                    logToRead?.let { f ->
                        val content = f.readText()
                        val lines = content.lines()
                        lines.forEachIndexed { idx, line ->
                            if (line.isNotBlank() && !shown.contains(idx)) {
                                shown.add(idx)
                                appendLog("[Go] $line")
                            }
                        }
                        lastSize = f.length()
                    }
                } catch (_: Exception) {}
            }

            // بعد از اتمام لاگ نهایی
            delay(800)
            try {
                val f = if (debugLog.exists()) debugLog else downloadsLog
                if (f.exists()) {
                    appendLog("=== Final Go Log ===")
                    f.readText().lines().forEach { line ->
                        if (line.isNotBlank()) appendLog("[Go] $line")
                    }
                }
            } catch (_: Exception) {}
        }

        // JNI در thread جداگانه
        jniExecutor.submit {
            val pingResult = try { FlowBridge.ping() } catch (e: Exception) { -999 }
                appendLog("[Kotlin] ping test: $pingResult (expected 42)")
                appendLog("[Kotlin] Calling JNI startTunnel...")
            try {
                val result = FlowBridge.startTunnel(
                    configJson,
                    credFile.absolutePath,
                    tokenFile.absolutePath
                )
                Thread.sleep(1000)
                appendLog("[Kotlin] startTunnel returned: $result")
                if (result == -2) appendLog("[ERROR] Login failed (code -2)")

            } catch (e: UnsatisfiedLinkError) {
                appendLog("[ERROR] UnsatisfiedLinkError: ${e.message}")
                Log.e("FlowService", "UnsatisfiedLinkError", e)
            } catch (e: Exception) {
                appendLog("[ERROR] ${e.javaClass.name}: ${e.message}")
                Log.e("FlowService", "JNI exception", e)
            } catch (e: Error) {
                appendLog("[ERROR] ${e.javaClass.name}: ${e.message}")
                Log.e("FlowService", "JNI error (Error)", e)
            } finally {
                isRunning = false
                onStatusChange?.invoke(false)
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }

        scope.launch {
            delay(7000)
            if (isRunning) {
                appendLog("[INFO] ✓ SOCKS5 فعال روی 127.0.0.1:1080")
                updateNotification("✓ متصل")
            }
        }
    }

    private fun stopTunnel() {
        appendLog("[Kotlin] Stopping...")
        try { FlowBridge.flowStop() } catch (_: Exception) {}
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
