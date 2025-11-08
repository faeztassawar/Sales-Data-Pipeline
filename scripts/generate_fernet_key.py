from cryptography.fernet import Fernet
import os

KEY_FILE = "fernet.key"

# Check if key file exists
if os.path.exists(KEY_FILE):
    with open(KEY_FILE, "rb") as f:
        key = f.read()
else:
    # Generate a new key and save it
    key = Fernet.generate_key()
    with open(KEY_FILE, "wb") as f:
        f.write(key)

print(key.decode())  # This will always print the same key after first run
