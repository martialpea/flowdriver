package com.flowdriver.service

/**
 * JNI bridge — نام متدها باید دقیقاً با export های Go مطابقت داشته باشه
 * Go export: Java_com_flowdriver_service_FlowBridge_startTunnel
 * Kotlin:    fun startTunnel(...)
 */
object FlowBridge {

    private var loaded = false

    fun load(): Boolean {
        if (loaded) return true
        return try {
            System.loadLibrary("flowdriver")
            loaded = true
            true
        } catch (e: UnsatisfiedLinkError) {
            android.util.Log.e("FlowBridge", "Failed to load libflowdriver.so: ${e.message}")
            false
        }
    }

    // startTunnel: string رو مستقیم پاس می‌کنیم — Go با C.GoString می‌خونه
    external fun startTunnel(configJson: String, tokenJson: String, credFilePath: String): Int
    external fun flowStop()
    external fun flowIsRunning(): Int
}
