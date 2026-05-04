package com.flowdriver.ui

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.view.View
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.flowdriver.databinding.ActivityMainBinding
import com.flowdriver.model.AppConfig
import com.flowdriver.model.TransportConfig
import com.flowdriver.service.FlowService
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONException
import org.json.JSONObject
import java.io.File

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val prefs by lazy { getSharedPreferences("flowdriver", MODE_PRIVATE) }

    // FIX: فقط token picker — نه credentials.json
    // کاربر فقط باید فایل .token رو import کنه
    private val pickToken = registerForActivityResult(ActivityResultContracts.GetContent()) { uri ->
        uri ?: return@registerForActivityResult
        lifecycleScope.launch(Dispatchers.IO) {
            try {
                val content = contentResolver.openInputStream(uri)?.bufferedReader()?.readText()
                    ?: throw Exception("فایل خالی است")

                // FIX: validation قبل از ذخیره
                validateTokenFile(content)

                val dest = File(filesDir, "credentials.json.token")
                dest.writeText(content)

                withContext(Dispatchers.Main) {
                    updateBadges()
                    toast("✓ Token وارد شد")
                }
            } catch (e: Exception) {
                withContext(Dispatchers.Main) {
                    toast("❌ خطا: ${e.message}")
                }
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        loadSettings()
        updateBadges()
        syncStatusUI(FlowService.isRunning)

        binding.btnImportToken.setOnClickListener { pickToken.launch("*/*") }
        binding.btnToggle.setOnClickListener { toggleTunnel() }
        binding.btnSaveSettings.setOnClickListener { saveSettings(); toast("✓ تنظیمات ذخیره شد") }
        binding.btnClearLog.setOnClickListener {
            binding.tvLog.text = ""
            FlowService.logLines.clear()
        }

        FlowService.onLogUpdate    = { line -> runOnUiThread { appendLog(line) } }
        FlowService.onStatusChange = { running -> runOnUiThread { syncStatusUI(running) } }
        FlowService.logLines.forEach { appendLog(it) }
    }

    // ── validation ────────────────────────────────────────────────────────────

    private fun validateTokenFile(content: String) {
        // FIX: بررسی JSON معتبر بودن
        try {
            val json = JSONObject(content)
            // token file باید refresh_token داشته باشه
            if (!json.has("refresh_token")) {
                throw Exception("این فایل credentials.json.token نیست\nفایل باید refresh_token داشته باشد")
            }
        } catch (e: JSONException) {
            throw Exception("فایل JSON معتبر نیست")
        }
    }

    // ── tunnel control ────────────────────────────────────────────────────────

    private fun toggleTunnel() {
        if (FlowService.isRunning) {
            startService(Intent(this, FlowService::class.java).apply {
                action = FlowService.ACTION_STOP
            })
            syncStatusUI(false)
            return
        }

        // بررسی token
        val tokenFile = File(filesDir, "credentials.json.token")
        if (!tokenFile.exists()) {
            toast("❌ ابتدا فایل credentials.json.token را import کنید")
            return
        }

        saveSettings()
        val tokenJson = tokenFile.readText()

        startForegroundService(Intent(this, FlowService::class.java).apply {
            action = FlowService.ACTION_START
            putExtra("config_json", buildConfig().toJson())
            putExtra("token_json", tokenJson)
        })
        syncStatusUI(true)
    }

    // ── config ────────────────────────────────────────────────────────────────

    private fun buildConfig() = AppConfig(
        listenAddr    = "127.0.0.1:1080",
        storageType   = "google",
        googleFolderId = binding.etFolderId.text.toString().trim(),
        refreshRateMs = binding.etRefreshRate.text.toString().toIntOrNull() ?: 200,
        flushRateMs   = binding.etFlushRate.text.toString().toIntOrNull() ?: 300,
        transport = TransportConfig(
            targetIP          = binding.etTargetIP.text.toString().trim(),
            sni               = binding.etSNI.text.toString().trim(),
            hostHeader        = binding.etHostHeader.text.toString().trim(),
            insecureSkipVerify = binding.switchInsecure.isChecked
        )
    )

    private fun saveSettings() {
        prefs.edit().apply {
            putString("folder_id",   binding.etFolderId.text.toString().trim())
            putInt("refresh_ms",     binding.etRefreshRate.text.toString().toIntOrNull() ?: 200)
            putInt("flush_ms",       binding.etFlushRate.text.toString().toIntOrNull() ?: 300)
            putString("target_ip",   binding.etTargetIP.text.toString().trim())
            putString("sni",         binding.etSNI.text.toString().trim())
            putString("host_header", binding.etHostHeader.text.toString().trim())
            putBoolean("insecure",   binding.switchInsecure.isChecked)
        }.apply()
    }

    private fun loadSettings() {
        binding.etFolderId.setText(prefs.getString("folder_id",   "1VBK3MfAe01Ir1Zm6NjnLFDGfqQHY9OH6"))
        binding.etRefreshRate.setText(prefs.getInt("refresh_ms",  200).toString())
        binding.etFlushRate.setText(prefs.getInt("flush_ms",      300).toString())
        binding.etTargetIP.setText(prefs.getString("target_ip",   "216.239.38.120:443"))
        binding.etSNI.setText(prefs.getString("sni",              "google.com"))
        binding.etHostHeader.setText(prefs.getString("host_header","www.googleapis.com"))
        binding.switchInsecure.isChecked = prefs.getBoolean("insecure", false)
    }

    // ── UI helpers ────────────────────────────────────────────────────────────

    private fun updateBadges() {
        val hasToken = File(filesDir, "credentials.json.token").exists()
        binding.tvTokenStatus.text = if (hasToken) "✓ وارد شده" else "✗ وارد نشده"
        binding.tvTokenStatus.setTextColor(
            getColor(if (hasToken) android.R.color.holo_green_dark else android.R.color.holo_red_dark)
        )
    }

    private fun syncStatusUI(running: Boolean) {
        binding.btnToggle.text = if (running) "قطع اتصال" else "اتصال"
        binding.btnToggle.setBackgroundColor(
            getColor(if (running) android.R.color.holo_red_light else android.R.color.holo_green_dark)
        )
        binding.tvStatus.text = if (running) "🟢 متصل" else "🔴 قطع"
        binding.cardSettings.alpha = if (running) 0.5f else 1f
        binding.btnImportToken.isEnabled = !running
    }

    private fun appendLog(line: String) {
        binding.tvLog.append("$line\n")
        binding.scrollLog.post { binding.scrollLog.fullScroll(View.FOCUS_DOWN) }
        val lines = binding.tvLog.text.lines()
        if (lines.size > 200) binding.tvLog.text = lines.takeLast(200).joinToString("\n")
    }

    private fun toast(msg: String) = Toast.makeText(this, msg, Toast.LENGTH_LONG).show()

    override fun onDestroy() {
        super.onDestroy()
        FlowService.onLogUpdate    = null
        FlowService.onStatusChange = null
    }
}
