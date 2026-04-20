import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(url, key)

email = "maxfriedman4321@gmail.com"
try:
    # Use admin API to get user by email
    user = supabase.auth.admin.list_users() # Not efficient but works if few users
    target_user = None
    for u in user:
        if u.email == email:
            target_user = u
            break
    
    if target_user:
        print(f"User found: {target_user.id}")
        print(f"Metadata: {target_user.user_metadata}")
    else:
        print("User not found.")
except Exception as e:
    print(f"Error: {e}")
