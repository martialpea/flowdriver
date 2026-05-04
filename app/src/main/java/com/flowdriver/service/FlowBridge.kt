package com.flowdriver.service

/**
 * Bridge به Go library
 * از نام‌های ساده به جای Java_ prefix استفاده می‌کنه
 */
object FlowBridge {

    init {
        System.loadLibrary("flowdriver")
    }

    external fun flowStart(configJson: String, tokenJson: String, credFilePath: String): Int
    external fun flowStop()
    external fun flowIsRunning(): Int
}
