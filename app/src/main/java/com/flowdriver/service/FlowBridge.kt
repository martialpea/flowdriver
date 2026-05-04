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

    // FIX: فقط configJson و credFilePath — نه tokenJson جداگانه
    // credFilePath = مسیر credentials.json
    // Go خودش credFilePath+".token" رو می‌خونه
    external fun startTunnel(configJson: String, credFilePath: String): Int
    external fun flowStop()
    external fun flowIsRunning(): Int
}
