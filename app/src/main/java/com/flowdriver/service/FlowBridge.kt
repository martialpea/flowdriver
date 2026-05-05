package com.flowdriver.service

object FlowBridge {
    private var loaded = false

    fun load(): Boolean {
        if (loaded) return true
        return try {
            System.loadLibrary("flowdriver")
            loaded = true
            true
        } catch (e: UnsatisfiedLinkError) {
            android.util.Log.e("FlowBridge", "load failed: ${e.message}")
            false
        }
    }

    external fun startTunnel(configJson: String, credFilePath: String, tokenFilePath: String): Int
    external fun flowStop()
    external fun flowIsRunning(): Int
    // لاگ‌های Go رو می‌گیریم
    external fun getLog(): String
}
