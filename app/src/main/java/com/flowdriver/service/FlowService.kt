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
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader

class FlowService : Service() {

    private var process: Process? = null
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
        // FIX: startForeground باید فوری صدا زده بشه — قبل از هر کار async
        // وگرنه اندروید بعد از 5 ثانیه ANR می‌ده و برنامه crash می‌کنه
        startForeground(NOTIF_ID, buildNotification("آماده اتصال..."))
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> {
                val configJson = intent.getStringExtra("config_json") ?: run {
                    appendLog("[ERROR] No config provided")
                    stopSelf()
                    return START_NOT_STICKY
                }
                val tokenJson = intent.getStringExtra("token_json")
                startTunnel(configJson, tokenJson)
            }
            ACTION_STOP -> stopTunnel()
        }
        return START_NOT_STICKY
    }

    private fun startTunnel(configJson: String, tokenJson: String?) {
        scope.launch {
            try {
                // ── ۱. extract binary ────────────────────────────────────────
                val binaryFile = extractBinary()
                if (binaryFile == null) {
                    appendLog("[ERROR] Binary not found in assets")
                    appendLog("[INFO]  Build the project via GitHub Actions first")
                    finishWithError()
                    return@launch
                }

                // ── ۲. نوشتن config ──────────────────────────────────────────
                val configFile = File(filesDir, "client_config.json")
                configFile.writeText(configJson)

                // ── ۳. بررسی token ───────────────────────────────────────────
                // FIX: فقط به token نیاز داریم — نه credentials.json کامل
                // credentials.json فقط برای OAuth اولیه لازمه که روی PC انجام میشه
                val tokenFile = File(filesDir, "credentials.json.token")

                if (tokenJson != null) {
                    // token از intent اومد — ذخیره‌اش کن
                    tokenFile.writeText(tokenJson)
                    appendLog("[INFO] Token loaded from import")
                } else if (!tokenFile.exists()) {
                    appendLog("[ERROR] credentials.json.token not found")
                    appendLog("[INFO]  Please import the token file first")
                    appendLog("[INFO]  Token is at: credentials.json.token on your PC")
                    finishWithError()
                    return@launch
                } else {
                    appendLog("[INFO] Using existing token file")
                }

                // credentials.json پایه رو از assets بخون (embed شده در APK)
                // این فقط client_id و client_secret هست — بدون secret token
                val credFile = File(filesDir, "credentials.json")
                if (!credFile.exists()) {
                    try {
                        assets.open("credentials.json").use { input ->
                            credFile.outputStream().use { input.copyTo(it) }
                        }
                        appendLog("[INFO] credentials.json loaded from APK assets")
                    } catch (e: Exception) {
                        appendLog("[WARN] credentials.json not in assets — token-only mode")
                        // بدون credentials.json هم کار می‌کنه اگه token داشته باشیم
                    }
                }

                // ── ۴. اجرای binary ──────────────────────────────────────────
                appendLog("[INFO] Starting FlowDriver...")
                appendLog("[INFO] ABI: ${android.os.Build.SUPPORTED_ABIS.firstOrNull()}")

                val cmdList = mutableListOf(
                    binaryFile.absolutePath,
                    "-c", configFile.absolutePath
                )
                if (credFile.exists()) {
                    cmdList += listOf("-gc", credFile.absolutePath)
                }

                process = ProcessBuilder(cmdList)
                    .directory(filesDir)
                    .redirectErrorStream(true)
                    .start()

                isRunning = true
                onStatusChange?.invoke(true)
                updateNotification("در حال اتصال...")

                // خواندن لاگ process
                val reader = BufferedReader(InputStreamReader(process!!.inputStream))
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    line?.let {
                        appendLog(it)
                        when {
                            it.contains("Listening for SOCKS5") ->
                                updateNotification("✓ متصل — SOCKS5 فعال")
                            it.contains("ERROR") || it.contains("error") ->
                                appendLog("[HINT] اگه خطا ادامه داشت token را دوباره import کنید")
                        }
                    }
                }

                val exitCode = process!!.waitFor()
                appendLog("[INFO] Process exited: $exitCode")
                if (exitCode != 0) {
                    appendLog("[HINT] Exit code $exitCode — احتمالاً token منقضی شده")
                    appendLog("[HINT] فایل credentials.json.token را از PC دوباره import کنید")
                }

            } catch (e: Exception) {
                appendLog("[ERROR] ${e.javaClass.simpleName}: ${e.message}")
                Log.e("FlowService", "Tunnel error", e)
            } finally {
                isRunning = false
                onStatusChange?.invoke(false)
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }
    }

    private fun stopTunnel() {
        appendLog("[INFO] Stopping tunnel...")
        process?.destroy()
        process = null
        isRunning = false
        onStatusChange?.invoke(false)
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun extractBinary(): File? {
        val abi = android.os.Build.SUPPORTED_ABIS.firstOrNull() ?: return null
        val assetName = when {
            abi.startsWith("arm64") -> "client_arm64"
            abi.startsWith("armeabi") -> "client_arm32"
            abi.startsWith("x86_64") -> "client_x86_64"
            else -> "client_arm64"
        }
        val outFile = File(filesDir, "flowclient")
        return try {
            assets.open(assetName).use { input ->
                outFile.outputStream().use { input.copyTo(it) }
            }
            outFile.setExecutable(true, true)
            appendLog("[INFO] Binary: $assetName")
            outFile
        } catch (e: Exception) {
            null
        }
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
            this, 0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE
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
        process?.destroy()
        scope.cancel()
        isRunning = false
    }
}
