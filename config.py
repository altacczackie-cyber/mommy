import os

# Token loaded from environment variable — NEVER hardcode it here
TOKEN = (os.environ.get("DISCORD_TOKEN") or "").strip().strip('"').strip("'")

if not TOKEN:
    TOKEN = input("Discord profile token: ").strip()

if not TOKEN:
    raise RuntimeError("[CONFIG] ERROR: DISCORD_TOKEN is missing or empty!")

print(f"[CONFIG] Token loaded: {TOKEN[:10]}...{TOKEN[-5:]} (len={len(TOKEN)})", flush=True)
