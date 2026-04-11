#!/bin/sh
## Reset WebUI credentials — next visit will show the setup wizard.
rm -f "${CONFIG_DIR:-/config}/webui_auth.json"
echo "WebUI credentials reset. Visit the WebUI to create new credentials."
