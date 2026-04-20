import os
import httpx
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")
user_id = "9f560459-ac58-4835-bd42-7b3e35fe880d" # Max's user ID
new_password = "Bob123Bob!"

print(f"Updating password for user {user_id}...")

# Supabase Auth Admin API: PATCH /auth/v1/admin/users/{user_id}
# https://supabase.com/docs/reference/api/admin-api-update-user
headers = {
    "apikey": key,
    "Authorization": f"Bearer {key}",
    "Content-Type": "application/json"
}

payload = {
    "password": new_password
}

r = httpx.put(f"{url}/auth/v1/admin/users/{user_id}", headers=headers, json=payload)

if r.status_code == 200:
    print("Password updated successfully.")
    print(r.json())
else:
    print(f"Failed to update password. Status: {r.status_code}")
    print(r.text)
