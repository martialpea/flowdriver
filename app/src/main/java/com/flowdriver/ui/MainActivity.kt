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
import java.io.File

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val prefs by lazy { getSharedPreferences("flowdriver", MODE_PRIVATE) }

    private val pickCredentials = registerForActivityResult(ActivityResultContracts.GetContent()) { uri ->
        uri ?: return@registerForActivityResult
        importFile(uri, "credentials.json") {
            prefs.edit().putBoolean("has_credentials", true).apply()
            updateBadges()
            toast("credentials.json وارد شد ✓")
        }
    }

    private val pickToken = registerForActivityResult(ActivityResultContracts.GetContent()) { uri ->
        uri ?: return@registerForActivityResult
        importFile(uri, "credentials.json.token") {
            prefs.edit().putBoolean("has_token", true).apply()
            updateBadges()
            toast("credentials.json.token وارد شد ✓")
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        loadSettings()
        updateBadges()
        syncStatusUI(FlowService.isRunning)

        binding.btnImportCredentials.setOnClickListener { pickCredentials.launch("application/json") }
        binding.btnImportToken.setOnClickListener { pickToken.launch("*/*") }
        binding.btnToggle.setOnClickListener { toggleTunnel() }
        binding.btnSaveSettings.setOnClickListener { saveSettings(); toast("تنظیمات ذخیره شد ✓") }
        binding.btnClearLog.setOnClickListener {
            binding.tvLog.text = ""
            FlowService.logLines.clear()
        }

        FlowService.onLogUpdate = { line -> runOnUiThread { appendLog(line) } }
        FlowService.onStatusChange = { running -> runOnUiThread { syncStatusUI(running) } }
        FlowService.logLines.forEach { appendLog(it) }
    }

    private fun toggleTunnel() {
        if (FlowService.isRunning) {
            stopService(Intent(this, FlowService::class.java).apply { action = FlowService.ACTION_STOP })
            syncStatusUI(false)
        } else {
            if (!File(filesDir, "credentials.json").exists()) {
                toast("ابتدا credentials.json را وارد کنید")
                return
            }
            saveSettings()
            val cfg = buildConfig()
            val tokenJson = File(filesDir, "credentials.json.token").let {
                if (it.exists()) it.readText() else null
            }
            startForegroundService(Intent(this, FlowService::class.java).apply {
                action = FlowService.ACTION_START
                putExtra("config_json", cfg.toJson())
                putExtra("token_json", tokenJson)
            })
            syncStatusUI(true)
        }
    }

    private fun buildConfig() = AppConfig(
        listenAddr = "127.0.0.1:1080",
        storageType = "google",
        googleFolderId = binding.etFolderId.text.toString().trim(),
        refreshRateMs = binding.etRefreshRate.text.toString().toIntOrNull() ?: 200,
        flushRateMs = binding.etFlushRate.text.toString().toIntOrNull() ?: 300,
        transport = TransportConfig(
            targetIP = binding.etTargetIP.text.toString().trim(),
            sni = binding.etSNI.text.toString().trim(),
            hostHeader = binding.etHostHeader.text.toString().trim(),
            insecureSkipVerify = binding.switchInsecure.isChecked
        )
    )

    private fun saveSettings() {
        prefs.edit().apply {
            putString("folder_id", binding.etFolderId.text.toString().trim())
            putInt("refresh_ms", binding.etRefreshRate.text.toString().toIntOrNull() ?: 200)
            putInt("flush_ms", binding.etFlushRate.text.toString().toIntOrNull() ?: 300)
            putString("target_ip", binding.etTargetIP.text.toString().trim())
            putString("sni", binding.etSNI.text.toString().trim())
            putString("host_header", binding.etHostHeader.text.toString().trim())
            putBoolean("insecure", binding.switchInsecure.isChecked)
        }.apply()
    }

    private fun loadSettings() {
        binding.etFolderId.setText(prefs.getString("folder_id", "1VBK3MfAe01Ir1Zm6NjnLFDGfqQHY9OH6"))
        binding.etRefreshRate.setText(prefs.getInt("refresh_ms", 200).toString())
        binding.etFlushRate.setText(prefs.getInt("flush_ms", 300).toString())
        binding.etTargetIP.setText(prefs.getString("target_ip", "216.239.38.120:443"))
        binding.etSNI.setText(prefs.getString("sni", "google.com"))
        binding.etHostHeader.setText(prefs.getString("host_header", "www.googleapis.com"))
        binding.switchInsecure.isChecked = prefs.getBoolean("insecure", false)
    }

    private fun importFile(uri: Uri, destName: String, onDone: () -> Unit) {
        lifecycleScope.launch(Dispatchers.IO) {
            try {
                val dest = File(filesDir, destName)
                contentResolver.openInputStream(uri)?.use { it.copyTo(dest.outputStream()) }
                withContext(Dispatchers.Main) { onDone() }
            } catch (e: Exception) {
                withContext(Dispatchers.Main) { toast("خطا: ${e.message}") }
            }
        }
    }

    private fun updateBadges() {
        val hasCred = File(filesDir, "credentials.json").exists()
        val hasToken = File(filesDir, "credentials.json.token").exists()
        binding.tvCredStatus.text = if (hasCred) "✓ وارد شده" else "✗ وارد نشده"
        binding.tvCredStatus.setTextColor(getColor(if (hasCred) android.R.color.holo_green_dark else android.R.color.holo_red_dark))
        binding.tvTokenStatus.text = if (hasToken) "✓ وارد شده" else "اختیاری"
        binding.tvTokenStatus.setTextColor(getColor(if (hasToken) android.R.color.holo_green_dark else android.R.color.darker_gray))
    }

    private fun syncStatusUI(running: Boolean) {
        binding.btnToggle.text = if (running) "قطع اتصال" else "اتصال"
        binding.btnToggle.setBackgroundColor(getColor(if (running) android.R.color.holo_red_light else android.R.color.holo_green_dark))
        binding.tvStatus.text = if (running) "🟢 متصل" else "🔴 قطع"
        binding.cardSettings.alpha = if (running) 0.5f else 1f
    }

    private fun appendLog(line: String) {
        binding.tvLog.append("$line\n")
        binding.scrollLog.post { binding.scrollLog.fullScroll(View.FOCUS_DOWN) }
        val lines = binding.tvLog.text.lines()
        if (lines.size > 200) binding.tvLog.text = lines.takeLast(200).joinToString("\n")
    }

    private fun toast(msg: String) = Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()

    override fun onDestroy() {
        super.onDestroy()
        FlowService.onLogUpdate = null
        FlowService.onStatusChange = null
    }
}
