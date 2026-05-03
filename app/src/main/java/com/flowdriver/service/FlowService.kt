package com.flowdriver.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import com.flowdriver.R
import com.flowdriver.model.AppConfig
import com.flowdriver.ui.MainActivity
import kotlinx.coroutines.*
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader

class FlowService : Service() {

    private var process: Process? = null
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val logBuffer = mutableListOf<String>()

    companion object {
        const val ACTION_START = "com.flowdriver.START"
        const val ACTION_STOP = "com.flowdriver.STOP"
        const val CHANNEL_ID = "flowdriver_channel"
        const val NOTIF_ID = 1
        const val MAX_LOG_LINES = 300
        var isRunning = false
        var logLines = mutableListOf<String>()
        var onLogUpdate: ((String) -> Unit)? = null
        var onStatusChange: ((Boolean) -> Unit)? = null
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> {
                val configJson = intent.getStringExtra("config_json") ?: return START_NOT_STICKY
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
                // کپی کردن binary به filesDir (باید executable باشه)
                val binaryFile = extractBinary()
                if (binaryFile == null) {
                    appendLog("[ERROR] Could not extract client binary")
                    return@launch
                }

                // نوشتن config.json
                val configFile = File(filesDir, "client_config.json")
                configFile.writeText(configJson)

                // نوشتن credentials.json (فایل پایه)
                val credFile = File(filesDir, "credentials.json")
                if (!credFile.exists()) {
                    // credentials.json پایه — کاربر باید آپلود کرده باشه
                    appendLog("[ERROR] credentials.json not found. Please import it first.")
                    onStatusChange?.invoke(false)
                    return@launch
                }

                // نوشتن credentials.json.token اگه داریم
                if (tokenJson != null) {
                    val tokenFile = File(filesDir, "credentials.json.token")
                    tokenFile.writeText(tokenJson)
                }

                appendLog("[INFO] Starting FlowDriver client...")
                appendLog("[INFO] Config: ${configFile.absolutePath}")

                val cmd = arrayOf(
                    binaryFile.absolutePath,
                    "-c", configFile.absolutePath,
                    "-gc", credFile.absolutePath
                )

                process = ProcessBuilder(*cmd)
                    .directory(filesDir)
                    .redirectErrorStream(true)
                    .start()

                isRunning = true
                onStatusChange?.invoke(true)
                startForeground(NOTIF_ID, buildNotification("در حال اتصال..."))

                // خواندن output برنامه
                val reader = BufferedReader(InputStreamReader(process!!.inputStream))
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    line?.let {
                        appendLog(it)
                        if (it.contains("Listening for SOCKS5")) {
                            updateNotification("متصل — SOCKS5 فعال است")
                        }
                    }
                }

                val exitCode = process!!.waitFor()
                appendLog("[INFO] Process exited with code $exitCode")

            } catch (e: Exception) {
                appendLog("[ERROR] ${e.message}")
                Log.e("FlowService", "Error", e)
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
        // نام فایل binary بر اساس ABI دستگاه
        val assetName = when {
            abi.startsWith("arm64") -> "client_arm64"
            abi.startsWith("armeabi") -> "client_arm32"
            abi.startsWith("x86_64") -> "client_x86_64"
            else -> "client_arm64"
        }

        val outFile = File(filesDir, "flowclient")
        return try {
            assets.open(assetName).use { input ->
                outFile.outputStream().use { output -> input.copyTo(output) }
            }
            outFile.setExecutable(true, true)
            appendLog("[INFO] Loaded binary: $assetName (ABI: $abi)")
            outFile
        } catch (e: Exception) {
            appendLog("[ERROR] Binary not found in assets: $assetName")
            appendLog("[INFO] Please add the compiled Go binary to app/src/main/assets/$assetName")
            null
        }
    }

    private fun appendLog(line: String) {
        val ts = java.text.SimpleDateFormat("HH:mm:ss", java.util.Locale.getDefault())
            .format(java.util.Date())
        val entry = "[$ts] $line"
        synchronized(logLines) {
            logLines.add(entry)
            if (logLines.size > MAX_LOG_LINES) logLines.removeAt(0)
        }
        onLogUpdate?.invoke(entry)
    }

    private fun createNotificationChannel() {
        val channel = NotificationChannel(
            CHANNEL_ID, "FlowDriver Tunnel",
            NotificationManager.IMPORTANCE_LOW
        ).apply { description = "FlowDriver tunnel status" }
        getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
    }

    private fun buildNotification(text: String): Notification {
        val intent = Intent(this, MainActivity::class.java)
        val pi = PendingIntent.getActivity(this, 0, intent, PendingIntent.FLAG_IMMUTABLE)
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("FlowDriver")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .setContentIntent(pi)
            .setOngoing(true)
            .build()
    }

    private fun updateNotification(text: String) {
        val nm = getSystemService(NotificationManager::class.java)
        nm.notify(NOTIF_ID, buildNotification(text))
    }

    override fun onDestroy() {
        super.onDestroy()
        process?.destroy()
        scope.cancel()
        isRunning = false
    }
}
