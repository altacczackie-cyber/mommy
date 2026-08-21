import datetime
import os
import shutil
import asyncio
import random
import discord
from discord.ext import commands
import config

LARGE_MEDIA = ""
CONTENT_FOLDER = ""
LOG_FILE = ""

# Max file size: 500 MB
MAX_FILE_SIZE = 500 * 1024 * 1024

bot = commands.Bot(command_prefix='!', self_bot=True)

# ── ANTI-DETECTION HELPERS ─────────────────────────────────────

def rand_delay(min_s=1.0, max_s=3.5):
    """Random delay between actions to avoid pattern detection."""
    return random.uniform(min_s, max_s)

def night_multiplier():
    """Slow down between 2am–8am UTC like a real person sleeping."""
    hour = datetime.datetime.utcnow().hour
    if 2 <= hour < 8:
        return random.uniform(2.5, 5.0)
    return 1.0

async def human_sleep(min_s=1.0, max_s=3.5):
    delay = rand_delay(min_s, max_s) * night_multiplier()
    await asyncio.sleep(delay)

# ── INIT ───────────────────────────────────────────────────────

def initialize():
    global CONTENT_FOLDER, LOG_FILE, LARGE_MEDIA

    current_dir = os.getcwd()
    CONTENT_FOLDER = os.path.join(current_dir, "media")
    LARGE_MEDIA = os.path.join(current_dir, "large")
    LOG_FILE = os.path.join(current_dir, "logs.log")

    prerequisite()


def prerequisite():
    if not os.path.exists(CONTENT_FOLDER):
        os.makedirs(CONTENT_FOLDER)

    if not os.path.exists(LARGE_MEDIA):
        os.makedirs(LARGE_MEDIA)

    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w') as file:
            file.write("----------------------------------------\n")
            file.write("        Time         Size (MB)    Name\n")
            file.write("----------------------------------------\n")


# ── UPLOAD ────────────────────────────────────────────────────

async def send_media_in_batches(channel, batch_size=1):
    batch_size = max(1, min(batch_size, 10))

    with open(LOG_FILE, 'r') as file:
        logs = file.read()

    content_files = [(os.path.join(CONTENT_FOLDER, f), f) for f in os.listdir(CONTENT_FOLDER) if os.path.isfile(os.path.join(CONTENT_FOLDER, f))]
    large_files = [(os.path.join(LARGE_MEDIA, f), f) for f in os.listdir(LARGE_MEDIA) if os.path.isfile(os.path.join(LARGE_MEDIA, f))]

    all_files_to_upload = content_files + large_files
    valid_files = []

    for file_path, filename in all_files_to_upload:
        if filename in logs:
            continue

        filesize_bytes = os.path.getsize(file_path)
        filesize_mb = filesize_bytes / (1024 * 1024)

        if filesize_bytes > MAX_FILE_SIZE:
            print(f"⚠️ Skipped {filename}: Size ({filesize_mb:.2f} MB) exceeds limit.")
            if os.path.dirname(file_path) == CONTENT_FOLDER:
                shutil.move(file_path, os.path.join(LARGE_MEDIA, filename))
            continue

        valid_files.append((file_path, filename, filesize_mb))

    # Shuffle order — real users don't upload in perfect alphabetical order
    random.shuffle(valid_files)

    total_files = len(valid_files)
    uploaded_count = 0

    for i in range(0, total_files, batch_size):
        batch = valid_files[i:i + batch_size]
        discord_files = [discord.File(f_path, f_name) for f_path, f_name, _ in batch]
        batch_names = ", ".join([f[1] for f in batch])
        print(f"📤 Uploading [{uploaded_count + 1}-{uploaded_count + len(batch)}/{total_files}]: {batch_names}")

        try:
            await channel.send(files=discord_files)
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(LOG_FILE, 'a') as file:
                for file_path, filename, filesize_mb in batch:
                    file.write(f"{current_time} {filesize_mb:10.2f}    {filename}\n")
                    print(f"Uploaded: {current_time} {filesize_mb:6.2f} MB  {filename}")

            uploaded_count += len(batch)

            # Random delay between uploads — not a fixed 1.5s
            await human_sleep(1.2, 4.0)

            # Occasionally take a longer pause like a real user would
            if random.random() < 0.12:
                pause = random.uniform(5, 15)
                print(f"⏸️ Taking a short break ({pause:.1f}s)...")
                await asyncio.sleep(pause)

        except Exception as e:
            print(f"Failed to upload batch ({batch_names}): {e}")
            for file_path, filename, _ in batch:
                if os.path.dirname(file_path) == CONTENT_FOLDER:
                    shutil.move(file_path, os.path.join(LARGE_MEDIA, filename))


def find_channel_by_id(channel_id: int):
    target = bot.get_channel(channel_id)
    if not target:
        for guild in bot.guilds:
            target = guild.get_channel(channel_id)
            if target:
                break
    return target


# ── EVENTS ───────────────────────────────────────────────────

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    print("\nCommands:")
    print("  • !tm <channel_id> [count]          -> Upload local folder to a channel")
    print("  • !tc <source_id> <target_id> [cnt] -> Transfer files between 2 channels")
    print("  • !kaboom [count]                   -> Upload local folder to current channel")
    print("  • !clone <old_id> <new_id>          -> Clone category")
    print("  • !bump                             -> Start auto-bumper\n")


@bot.event
async def on_message(message):
    if message.author != bot.user:
        return

    # 1. Transfer Channel to Channel (!tc <source_id> <target_id> [count])
    if message.content.startswith("!tc"):
        parts = message.content.split()
        if len(parts) >= 3:
            try:
                source_id = int(parts[1])
                target_id = int(parts[2])
                batch_size = max(1, min(int(parts[3]), 10)) if len(parts) >= 4 else 1

                await message.delete()

                source_channel = find_channel_by_id(source_id)
                target_channel = find_channel_by_id(target_id)

                if not source_channel or not target_channel:
                    print("❌ One or both channel IDs were not found.")
                    return

                print(f"🔄 Moving media from #{source_channel.name} to #{target_channel.name}...")

                file_batch = []
                total_transferred = 0

                async for msg in source_channel.history(limit=None, oldest_first=True):
                    for attachment in msg.attachments:
                        if attachment.size > MAX_FILE_SIZE:
                            print(f"⚠️ Skipped oversized file: {attachment.filename}")
                            continue

                        try:
                            discord_file = await attachment.to_file()
                            file_batch.append(discord_file)
                        except Exception as fetch_err:
                            print(f"❌ Failed downloading {attachment.filename}: {fetch_err}")
                            continue

                        if len(file_batch) >= batch_size:
                            try:
                                await target_channel.send(files=file_batch)
                                total_transferred += len(file_batch)
                                print(f"📤 Sent {len(file_batch)} file(s) (Total: {total_transferred})")
                                file_batch = []
                                await human_sleep(1.5, 4.0)
                            except Exception as send_err:
                                print(f"❌ Send failed: {send_err}")
                                file_batch = []

                if file_batch:
                    try:
                        await target_channel.send(files=file_batch)
                        total_transferred += len(file_batch)
                    except Exception as send_err:
                        print(f"❌ Final batch send failed: {send_err}")

                print(f"✅ Transfer complete! Total: {total_transferred} files.")

            except ValueError:
                print("Usage: !tc <source_id> <target_id> [count]")

    # 2. Transfer Local Media to Channel (!tm <channel_id> [count])
    elif message.content.startswith("!tm"):
        parts = message.content.split()
        if len(parts) >= 2:
            try:
                target_channel_id = int(parts[1])
                batch_size = int(parts[2]) if len(parts) >= 3 else 1
                await message.delete()

                target_channel = find_channel_by_id(target_channel_id)
                if not target_channel:
                    print("❌ Target channel not found.")
                    return

                print(f"🔄 Uploading local media to #{target_channel.name} ({batch_size}/msg)...")
                await send_media_in_batches(target_channel, batch_size=batch_size)
                print("\n✅ Upload Complete!")
            except ValueError:
                print("Usage: !tm <channel_id> [count]")

    # 3. Clone Category (!clone <old_id> <new_id>)
    elif message.content.startswith("!clone"):
        parts = message.content.split()
        if len(parts) == 3:
            try:
                old_category_id = int(parts[1])
                new_category_id = int(parts[2])
                guild = message.guild
                await message.delete()

                source_category = next(
                    (g.get_channel(old_category_id) for g in bot.guilds
                     if isinstance(g.get_channel(old_category_id), discord.CategoryChannel)),
                    None
                )
                target_category = guild.get_channel(new_category_id)

                if not source_category or not target_category or not isinstance(target_category, discord.CategoryChannel):
                    print("❌ Invalid source or target category ID.")
                    return

                print(f"🔄 Cloning channels from {source_category.name} to {target_category.name}...")
                existing_channel_names = {c.name.lower() for c in target_category.text_channels}

                for channel in source_category.text_channels:
                    try:
                        if channel.name.lower() in existing_channel_names:
                            continue

                        new_channel = await guild.create_text_channel(channel.name, category=target_category, nsfw=True)
                        print(f"Cloning: {channel.name} -> {new_channel.name}")

                        messages = [m async for m in channel.history(limit=None, oldest_first=True)]
                        for m in messages:
                            content_to_send = f"**{m.author.name}**: {m.content}"
                            files = [await a.to_file() for a in m.attachments]
                            if content_to_send.strip() or files:
                                await new_channel.send(content=content_to_send if m.content else None, files=files)
                                await human_sleep(0.8, 2.5)
                    except Exception as e:
                        print(f"Error copying {channel.name}: {e}")

                print(f"✅ Finished cloning into {target_category.name}!")
            except ValueError:
                print("Usage: !clone <old_category_id> <new_category_id>")

    # 4. Kaboom (!kaboom [count])
    elif message.content.startswith("!kaboom"):
        parts = message.content.split()
        batch_size = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1

        try:
            await message.delete()
        except discord.errors.NotFound:
            pass

        print(f"🔄 Uploading local media to current channel ({batch_size}/msg)...")
        try:
            await send_media_in_batches(message.channel, batch_size=batch_size)
            print("\n✅ Upload Complete!")
        except Exception as ex:
            print(f"Error: {ex}")

    # 5. Bump (!bump)
    elif message.content.startswith("!bump"):
        try:
            await message.delete()
        except discord.errors.NotFound:
            pass

        channel = message.channel
        print("🤖 Auto-bump active.")
        disboard_bot_id = 302050872383242240

        while True:
            try:
                app_commands = await channel.application_commands()
                disboard_cmd = next(
                    (cmd for cmd in app_commands if cmd.name == "bump" and cmd.application_id == disboard_bot_id),
                    None
                )
                if disboard_cmd:
                    await disboard_cmd()
                    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Executed /bump.")
                else:
                    print("⚠️ /bump command not found.")
            except Exception as e:
                print(f"Bump error: {e}")

            # Random wait between 1h55m and 2h10m — not exactly 2 hours
            wait = random.randint(6900, 7800)
            print(f"⏳ Next bump in {wait // 60}m {wait % 60}s")
            await asyncio.sleep(wait)


if __name__ == '__main__':
    initialize()
    TOKEN = getattr(config, 'TOKEN', None) or input("Discord profile token: ")
    bot.run(TOKEN)
