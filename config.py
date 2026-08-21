import os

# Token loaded from environment variable — NEVER hardcode it here
TOKEN = os.environ.get("DISCORD_TOKEN") or input("Discord profile token: ")
