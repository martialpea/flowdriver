package com.flowdriver.model

import com.google.gson.Gson
import com.google.gson.annotations.SerializedName

data class TransportConfig(
    @SerializedName("TargetIP") val targetIP: String = "216.239.38.120:443",
    @SerializedName("SNI") val sni: String = "google.com",
    @SerializedName("HostHeader") val hostHeader: String = "www.googleapis.com",
    @SerializedName("InsecureSkipVerify") val insecureSkipVerify: Boolean = false
)

data class AppConfig(
    @SerializedName("listen_addr") val listenAddr: String = "127.0.0.1:1080",
    @SerializedName("storage_type") val storageType: String = "google",
    @SerializedName("google_folder_id") val googleFolderId: String = "",
    @SerializedName("refresh_rate_ms") val refreshRateMs: Int = 200,
    @SerializedName("flush_rate_ms") val flushRateMs: Int = 300,
    @SerializedName("transport") val transport: TransportConfig = TransportConfig()
) {
    fun toJson(): String = Gson().toJson(this)
    companion object {
        fun fromJson(json: String): AppConfig = try {
            Gson().fromJson(json, AppConfig::class.java)
        } catch (e: Exception) { AppConfig() }
    }
}

data class UserSettings(
    val googleFolderId: String = "1VBK3MfAe01Ir1Zm6NjnLFDGfqQHY9OH6",
    val refreshRateMs: Int = 200,
    val flushRateMs: Int = 300,
    val targetIP: String = "216.239.38.120:443",
    val sni: String = "google.com",
    val hostHeader: String = "www.googleapis.com",
    val insecureSkipVerify: Boolean = false,
    val hasToken: Boolean = false
)
