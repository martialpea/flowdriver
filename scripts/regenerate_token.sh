#!/bin/bash
# این script رو روی PC اجرا کن تا token file جدید با client_id بسازه
# بعد همون فایل credentials.json.token رو به اندروید import کن

echo "Regenerating token with client_id embedded..."
./client -c client_config.json -gc credentials.json

echo ""
echo "Done! Now import credentials.json.token to Android app"
