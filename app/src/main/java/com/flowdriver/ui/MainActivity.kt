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

    // import credentials.json
    private val pickCredentials = registerForActivityResult(ActivityResultContracts.GetContent()) { uri ->
        uri ?: return@registerForActivityResult
        importFile(uri, "credentials.json", validate = { content ->
            val json = JSONObject(content)
            if (!json.has("installed")) throw Exception("فایل credentials.json معتبر نیست")
        }) { toast("✓ credentials.json وارد شد") }
    }

    // import credentials.json.token
    private val pickToken = registerForActivityResult(ActivityResultContracts.GetContent()) { uri ->
        uri ?: return@registerForActivityResult
        importFile(uri, "credentials.json.token", validate = { content ->
            val json = JSONObject(content)
            if (!json.has("refresh_token")) throw Exception("فایل token معتبر نیست — refresh_token ندارد")
        }) { toast("✓ credentials.json.token وارد شد") }
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
        binding.btnSaveSettings.setOnClickListener { saveSettings(); toast("✓ ذخیره شد") }
        binding.btnClearLog.setOnClickListener {
            binding.tvLog.text = ""
            FlowService.logLines.clear()
        }

        FlowService.onLogUpdate    = { line -> runOnUiThread { appendLog(line) } }
        FlowService.onStatusChange = { running -> runOnUiThread { syncStatusUI(running) } }
        FlowService.logLines.forEach { appendLog(it) }
    }

    private fun toggleTunnel() {
        if (FlowService.isRunning) {
            startService(Intent(this, FlowService::class.java).apply { action = FlowService.ACTION_STOP })
            return
        }

        // بررسی هر دو فایل
        val credFile  = File(filesDir, "credentials.json")
        val tokenFile = File(filesDir, "credentials.json.token")

        if (!credFile.exists()) {
            toast("❌ ابتدا credentials.json را import کنید")
            return
        }
        if (!tokenFile.exists()) {
            toast("❌ ابتدا credentials.json.token را import کنید")
            return
        }

        saveSettings()
        val tokenJson = tokenFile.readText()

        startForegroundService(Intent(this, FlowService::class.java).apply {
            action = FlowService.ACTION_START
            putExtra("config_json", buildConfig().toJson())
            putExtra("token_json", tokenJson)
        })
    }

    private fun buildConfig() = AppConfig(
        listenAddr     = "127.0.0.1:1080",
        storageType    = "google",
        googleFolderId = binding.etFolderId.text.toString().trim(),
        refreshRateMs  = binding.etRefreshRate.text.toString().toIntOrNull() ?: 200,
        flushRateMs    = binding.etFlushRate.text.toString().toIntOrNull() ?: 300,
        transport = TransportConfig(
            targetIP           = binding.etTargetIP.text.toString().trim(),
            sni                = binding.etSNI.text.toString().trim(),
            hostHeader         = binding.etHostHeader.text.toString().trim(),
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
        binding.etFolderId.setText(prefs.getString("folder_id",    "1VBK3MfAe01Ir1Zm6NjnLFDGfqQHY9OH6"))
        binding.etRefreshRate.setText(prefs.getInt("refresh_ms",   200).toString())
        binding.etFlushRate.setText(prefs.getInt("flush_ms",       300).toString())
        binding.etTargetIP.setText(prefs.getString("target_ip",    "216.239.38.120:443"))
        binding.etSNI.setText(prefs.getString("sni",               "google.com"))
        binding.etHostHeader.setText(prefs.getString("host_header", "www.googleapis.com"))
        binding.switchInsecure.isChecked = prefs.getBoolean("insecure", false)
    }

    private fun importFile(uri: Uri, destName: String, validate: (String) -> Unit, onDone: () -> Unit) {
        lifecycleScope.launch(Dispatchers.IO) {
            try {
                val content = contentResolver.openInputStream(uri)?.bufferedReader()?.readText()
                    ?: throw Exception("فایل خالی است")
                validate(content)
                File(filesDir, destName).writeText(content)
                withContext(Dispatchers.Main) {
                    updateBadges()
                    onDone()
                }
            } catch (e: JSONException) {
                withContext(Dispatchers.Main) { toast("❌ JSON نامعتبر") }
            } catch (e: Exception) {
                withContext(Dispatchers.Main) { toast("❌ ${e.message}") }
            }
        }
    }

    private fun updateBadges() {
        val hasCred  = File(filesDir, "credentials.json").exists()
        val hasToken = File(filesDir, "credentials.json.token").exists()

        binding.tvCredStatus.text = if (hasCred) "✓ وارد شده" else "✗ وارد نشده"
        binding.tvCredStatus.setTextColor(getColor(if (hasCred) android.R.color.holo_green_dark else android.R.color.holo_red_dark))

        binding.tvTokenStatus.text = if (hasToken) "✓ وارد شده" else "✗ وارد نشده"
        binding.tvTokenStatus.setTextColor(getColor(if (hasToken) android.R.color.holo_green_dark else android.R.color.holo_red_dark))
    }

    private fun syncStatusUI(running: Boolean) {
        binding.btnToggle.text = if (running) "قطع اتصال" else "اتصال"
        binding.btnToggle.setBackgroundColor(
            getColor(if (running) android.R.color.holo_red_light else android.R.color.holo_green_dark)
        )
        binding.tvStatus.text = if (running) "🟢 متصل" else "🔴 قطع"
        binding.cardSettings.alpha = if (running) 0.5f else 1f
        binding.btnImportCredentials.isEnabled = !running
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
