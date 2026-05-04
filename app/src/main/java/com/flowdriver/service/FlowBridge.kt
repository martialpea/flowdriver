package com.flowdriver.service

/**
 * JNI bridge به Go library
 * libflowdriver.so داخل APK embed می‌شه و اندروید آن را load می‌کند
 * نیازی به binary جداگانه نیست — مستقیم داخل APK اجرا می‌شه
 */
object FlowBridge {

    init {
        System.loadLibrary("flowdriver")
    }

    // توابع Go که از JNI export شدن
    external fun start(configJson: String, tokenJson: String, credFilePath: String): Int
    external fun stop()
    external fun isRunning(): Int
}
