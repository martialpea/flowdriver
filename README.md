# FlowDriver Android

این پروژه APK اندروید FlowDriver را می‌سازد.

## راه‌اندازی

1. محتوای FlowDriver-v2 (کد Go) را در ریشه ریپو قرار دهید
2. محتوای این پوشه را هم در ریشه قرار دهید
3. یک tag بزنید:

```
git tag v1.0.0
git push origin v1.0.0
```

GitHub Actions به صورت خودکار:
- Go binary ها برای arm64/arm32/x86_64 می‌سازد
- Binary ها را داخل assets کپی می‌کند
- APK نهایی می‌سازد
- همه را در Releases قرار می‌دهد

## استفاده از APK

1. APK را نصب کنید
2. credentials.json را وارد کنید
3. اگه token دارید آن را هم وارد کنید
4. تنظیمات را بررسی و ذخیره کنید
5. دکمه اتصال را بزنید
6. در SocksDroid آدرس 127.0.0.1:1080 را تنظیم کنید
