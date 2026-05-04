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
        startForeground(NOTIF_ID, buildNotification("آماده اتصال..."))
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> {
                val configJson = intent.getStringExtra("config_json") ?: run {
                    appendLog("[ERROR] No config provided")
                    stopSelf(); return START_NOT_STICKY
                }
                startTunnel(configJson, intent.getStringExtra("token_json"))
            }
            ACTION_STOP -> stopTunnel()
        }
        return START_NOT_STICKY
    }

    private fun startTunnel(configJson: String, tokenJson: String?) {
        scope.launch {
            try {
                // ── پیدا کردن مکان قابل اجرا ─────────────────────────────────
                // filesDir روی بعضی دستگاه‌ها noexec است
                // راه‌حل: کپی به nativeLibraryDir یا codeCacheDir که exec مجاز است
                val binaryFile = extractBinaryToExecDir()
                if (binaryFile == null) {
                    appendLog("[ERROR] Cannot find executable location")
                    appendLog("[INFO]  Device may restrict binary execution")
                    finishWithError(); return@launch
                }

                // نوشتن فایل‌ها در filesDir (فقط read/write — نه exec)
                val configFile = File(filesDir, "client_config.json")
                configFile.writeText(configJson)

                val tokenFile = File(filesDir, "credentials.json.token")
                if (tokenJson != null) {
                    tokenFile.writeText(tokenJson)
                } else if (!tokenFile.exists()) {
                    appendLog("[ERROR] Token file not found")
                    finishWithError(); return@launch
                }

                // credentials.json از assets اگه داریم
                val credFile = File(filesDir, "credentials.json")
                if (!credFile.exists()) {
                    try {
                        assets.open("credentials.json").use { it.copyTo(credFile.outputStream()) }
                    } catch (_: Exception) {}
                }

                appendLog("[INFO] Binary: ${binaryFile.absolutePath}")
                appendLog("[INFO] Executable: ${binaryFile.canExecute()}")
                appendLog("[INFO] Starting FlowDriver...")

                val cmd = mutableListOf(
                    binaryFile.absolutePath,
                    "-c", configFile.absolutePath
                )
                if (credFile.exists()) cmd += listOf("-gc", credFile.absolutePath)

                process = ProcessBuilder(cmd)
                    .directory(filesDir)
                    .redirectErrorStream(true)
                    .start()

                isRunning = true
                onStatusChange?.invoke(true)
                updateNotification("در حال اتصال...")

                val reader = BufferedReader(InputStreamReader(process!!.inputStream))
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    line?.let {
                        appendLog(it)
                        if (it.contains("Listening for SOCKS5"))
                            updateNotification("✓ متصل — SOCKS5 فعال")
                    }
                }

                val exit = process!!.waitFor()
                appendLog("[INFO] Process exited: $exit")
                if (exit == 13) {
                    appendLog("[ERROR] Permission denied (exit 13)")
                    appendLog("[HINT]  دستگاه شما اجرای binary را مسدود کرده")
                }

            } catch (e: Exception) {
                appendLog("[ERROR] ${e.javaClass.simpleName}: ${e.message}")
                Log.e("FlowService", "error", e)
            } finally {
                isRunning = false
                onStatusChange?.invoke(false)
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }
    }

    /**
     * سه مکان رو به ترتیب امتحان می‌کنه:
     * 1. codeCacheDir    — معمولاً exec مجاز است (API 21+)
     * 2. nativeLibraryDir— همیشه exec مجاز است اما read-only است
     * 3. filesDir         — fallback اما ممکن است noexec باشد
     */
    private fun extractBinaryToExecDir(): File? {
        val abi = android.os.Build.SUPPORTED_ABIS.firstOrNull() ?: return null
        val assetName = when {
            abi.startsWith("arm64")   -> "client_arm64"
            abi.startsWith("armeabi") -> "client_arm32"
            abi.startsWith("x86_64")  -> "client_x86_64"
            else                       -> "client_arm64"
        }
        appendLog("[INFO] Device ABI: $abi -> $assetName")

        // مکان‌های امتحانی به ترتیب اولویت
        val candidates = listOf(
            File(codeCacheDir, "flowclient"),   // بهترین گزینه
            File(filesDir,     "flowclient"),   // fallback
            File(cacheDir,     "flowclient")    // آخرین گزینه
        )

        for (dest in candidates) {
            try {
                appendLog("[INFO] Trying: ${dest.absolutePath}")
                assets.open(assetName).use { it.copyTo(dest.outputStream()) }
                dest.setExecutable(true, true)
                dest.setReadable(true, false)

                // تست واقعی — آیا می‌توان اجرا کرد؟
                if (canExecuteFile(dest)) {
                    appendLog("[INFO] ✓ Executable location: ${dest.parent}")
                    return dest
                } else {
                    appendLog("[WARN] noexec at: ${dest.parent}")
                    dest.delete()
                }
            } catch (e: Exception) {
                appendLog("[WARN] Failed ${dest.parent}: ${e.message}")
            }
        }
        return null
    }

    /**
     * تست واقعی اجرای فایل با یه دستور بی‌ضرر
     */
    private fun canExecuteFile(file: File): Boolean {
        return try {
            // اجرای binary با --help یا بدون argument برای تست سریع
            val p = ProcessBuilder(file.absolutePath, "--version-check-only-for-test")
                .redirectErrorStream(true)
                .start()
            // منتظر ۱ ثانیه می‌مونیم
            val exited = p.waitFor(1, java.util.concurrent.TimeUnit.SECONDS)
            p.destroy()
            // اگه با permission denied (13) خارج شد، false برگردون
            // اگه با هر exit code دیگه‌ای خارج شد، یعنی اجرا شد
            if (exited) {
                val code = p.exitValue()
                code != 13 && code != 126 && code != 127
            } else {
                // تایم‌اوت یعنی داره اجرا می‌شه — این خوبه
                true
            }
        } catch (e: Exception) {
            false
        }
    }

    private fun stopTunnel() {
        appendLog("[INFO] Stopping...")
        process?.destroy()
        process = null
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
        val pi = PendingIntent.getActivity(this, 0,
            Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE)
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
        process?.destroy()
        scope.cancel()
        isRunning = false
    }
}
