import datetime
import os
import shutil
import asyncio
import random
import traceback
import discord
from discord.ext import commands
import config

LARGE_MEDIA    = ""
CONTENT_FOLDER = ""
LOG_FILE       = ""

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB

bot = commands.Bot(command_prefix='!', self_bot=True)

# ── LOGGING ───────────────────────────────────────────────────
# Plain text, no ANSI/emoji — Railway-friendly

def ts():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def log(level, msg):
    print(f"[{ts()}] [{level}] {msg}", flush=True)

def log_info(msg):  log("INFO",  msg)
def log_warn(msg):  log("WARN",  msg)
def log_error(msg): log("ERROR", msg)
def log_ok(msg):    log("OK",    msg)

# ── ANTI-DETECTION ────────────────────────────────────────────

def night_multiplier():
    hour = datetime.datetime.utcnow().hour
    if 2 <= hour < 8:
        return random.uniform(2.5, 5.0)
    return 1.0

async def human_sleep(min_s=1.0, max_s=3.5):
    await asyncio.sleep(random.uniform(min_s, max_s) * night_multiplier())

# ── INIT ──────────────────────────────────────────────────────

def initialize():
    global CONTENT_FOLDER, LOG_FILE, LARGE_MEDIA
    cwd = os.getcwd()
    CONTENT_FOLDER = os.path.join(cwd, "media")
    LARGE_MEDIA    = os.path.join(cwd, "large")
    LOG_FILE       = os.path.join(cwd, "logs.log")

    for path in [CONTENT_FOLDER, LARGE_MEDIA]:
        os.makedirs(path, exist_ok=True)

    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w') as f:
            f.write("timestamp            size_mb    filename\n")
            f.write("-" * 50 + "\n")

# ── HELPERS ───────────────────────────────────────────────────

def find_channel_by_id(channel_id: int):
    ch = bot.get_channel(channel_id)
    if ch:
        return ch
    for guild in bot.guilds:
        ch = guild.get_channel(channel_id)
        if ch:
            return ch
    return None

def find_category(category_id: int):
    for guild in bot.guilds:
        ch = guild.get_channel(category_id)
        if isinstance(ch, discord.CategoryChannel):
            return ch
    return None

# ── UPLOAD ────────────────────────────────────────────────────

async def send_media_in_batches(channel, batch_size=1):
    batch_size = max(1, min(batch_size, 10))

    with open(LOG_FILE, 'r') as f:
        already_uploaded = f.read()

    all_files = []
    for folder in [CONTENT_FOLDER, LARGE_MEDIA]:
        for fname in os.listdir(folder):
            fpath = os.path.join(folder, fname)
            if not os.path.isfile(fpath):
                continue
            if fname in already_uploaded:
                continue
            size_bytes = os.path.getsize(fpath)
            size_mb    = size_bytes / (1024 * 1024)
            if size_bytes > MAX_FILE_SIZE:
                log_warn(f"Skipped {fname} ({size_mb:.1f} MB) — exceeds 500 MB limit")
                if folder == CONTENT_FOLDER:
                    shutil.move(fpath, os.path.join(LARGE_MEDIA, fname))
                continue
            all_files.append((fpath, fname, size_mb))

    random.shuffle(all_files)
    total = len(all_files)

    if total == 0:
        log_info("No new files to upload.")
        return

    log_info(f"Starting upload of {total} file(s) in batches of {batch_size}")
    uploaded = 0
    failed   = 0

    for i in range(0, total, batch_size):
        batch = all_files[i:i + batch_size]
        names = ", ".join(f[1] for f in batch)
        log_info(f"Uploading batch [{i+1}-{min(i+batch_size, total)}/{total}]: {names}")

        discord_files = []
        for fpath, fname, _ in batch:
            try:
                discord_files.append(discord.File(fpath, fname))
            except Exception as e:
                log_error(f"Could not open file {fname}: {e}")
                failed += 1

        if not discord_files:
            continue

        try:
            await channel.send(files=discord_files)
            now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            with open(LOG_FILE, 'a') as f:
                for _, fname, size_mb in batch:
                    f.write(f"{now}  {size_mb:8.2f}    {fname}\n")
            uploaded += len(discord_files)
            log_ok(f"Sent {len(discord_files)} file(s) (total uploaded: {uploaded})")
        except discord.HTTPException as e:
            log_error(f"HTTP error sending batch [{names}]: {e.status} {e.text}")
            failed += len(discord_files)
            for fpath, fname, _ in batch:
                if os.path.dirname(fpath) == CONTENT_FOLDER:
                    shutil.move(fpath, os.path.join(LARGE_MEDIA, fname))
        except Exception as e:
            log_error(f"Unexpected error sending batch [{names}]: {e}")
            log_error(traceback.format_exc())
            failed += len(discord_files)

        await human_sleep(1.2, 4.0)
        if random.random() < 0.12:
            pause = random.uniform(5, 15)
            log_info(f"Short break for {pause:.1f}s...")
            await asyncio.sleep(pause)

    log_ok(f"Upload done. Uploaded: {uploaded} | Failed: {failed} | Total: {total}")

# ── CLONE (single category) ───────────────────────────────────

async def clone_category(source_cat: discord.CategoryChannel, target_cat: discord.CategoryChannel, guild: discord.Guild):
    log_info(f"Cloning category '{source_cat.name}' ({source_cat.id}) -> '{target_cat.name}' ({target_cat.id})")
    existing = {c.name.lower() for c in target_cat.text_channels}
    channels = list(source_cat.text_channels)
    log_info(f"Found {len(channels)} text channel(s) to clone")

    cloned   = 0
    skipped  = 0
    msgs_ok  = 0
    msgs_err = 0

    for ch in channels:
        if ch.name.lower() in existing:
            log_warn(f"Skipping #{ch.name} — already exists in target category")
            skipped += 1
            continue

        try:
            new_ch = await guild.create_text_channel(ch.name, category=target_cat, nsfw=True)
            log_info(f"Created #{new_ch.name}")
            cloned += 1
        except discord.Forbidden:
            log_error(f"No permission to create #{ch.name} — skipping")
            skipped += 1
            continue
        except discord.HTTPException as e:
            log_error(f"Failed to create #{ch.name}: {e.status} {e.text}")
            skipped += 1
            continue
        except Exception as e:
            log_error(f"Unexpected error creating #{ch.name}: {e}")
            log_error(traceback.format_exc())
            skipped += 1
            continue

        # Copy messages
        log_info(f"Copying messages from #{ch.name}...")
        msg_count = 0

        try:
            async for m in ch.history(limit=None, oldest_first=True):
                content = m.content or ""
                files   = []

                for att in m.attachments:
                    try:
                        files.append(await att.to_file())
                    except Exception as e:
                        log_warn(f"Could not download attachment {att.filename} from #{ch.name}: {e}")

                # Send in chunks of 10 attachments max
                if files:
                    for j in range(0, len(files), 10):
                        chunk = files[j:j + 10]
                        try:
                            await new_ch.send(
                                content=f"**{m.author.name}**: {content}" if content and j == 0 else None,
                                files=chunk,
                            )
                            msgs_ok += 1
                        except discord.HTTPException as e:
                            log_error(f"HTTP error sending message in #{ch.name}: {e.status} {e.text}")
                            msgs_err += 1
                        except Exception as e:
                            log_error(f"Error sending message in #{ch.name}: {e}")
                            msgs_err += 1
                        await human_sleep(0.8, 2.0)
                elif content.strip():
                    try:
                        await new_ch.send(content=f"**{m.author.name}**: {content}")
                        msgs_ok += 1
                    except discord.HTTPException as e:
                        log_error(f"HTTP error sending text in #{ch.name}: {e.status} {e.text}")
                        msgs_err += 1
                    except Exception as e:
                        log_error(f"Error sending text in #{ch.name}: {e}")
                        msgs_err += 1
                    await human_sleep(0.5, 1.5)

                msg_count += 1
                if msg_count % 50 == 0:
                    log_info(f"  #{ch.name}: {msg_count} messages processed so far...")

        except discord.Forbidden:
            log_error(f"No read permission for #{ch.name} — skipping messages")
        except discord.HTTPException as e:
            log_error(f"HTTP error reading history of #{ch.name}: {e.status} {e.text}")
        except Exception as e:
            log_error(f"Unexpected error reading #{ch.name}: {e}")
            log_error(traceback.format_exc())

        log_ok(f"#{ch.name} done. Messages processed: {msg_count}")
        await human_sleep(1.0, 3.0)

    log_ok(
        f"Category '{source_cat.name}' clone complete. "
        f"Channels cloned: {cloned} | Skipped: {skipped} | "
        f"Messages sent: {msgs_ok} | Errors: {msgs_err}"
    )

# ── EVENTS ────────────────────────────────────────────────────

@bot.event
async def on_ready():
    log_ok(f"Logged in as {bot.user} ({bot.user.id})")
    log_info("Commands:")
    log_info("  !tm <channel_id> [batch]                      Upload media/ to a channel")
    log_info("  !tc <src_id> <dst_id> [batch]                 Transfer files channel->channel")
    log_info("  !kaboom [batch]                               Upload media/ to current channel")
    log_info("  !clone <src_cat_id> <dst_cat_id>              Clone one category")
    log_info("  !clones <dst_cat_id> <src1> <src2> ...        Clone multiple categories into one")
    log_info("  !bump                                         Start auto-bumper")


@bot.event
async def on_message(message):
    if message.author != bot.user:
        return

    content = message.content.strip()

    # ── !tc ───────────────────────────────────────────────────
    if content.startswith("!tc"):
        parts = content.split()
        if len(parts) < 3:
            log_warn("Usage: !tc <source_id> <target_id> [batch_size]")
            return
        try:
            source_id  = int(parts[1])
            target_id  = int(parts[2])
            batch_size = max(1, min(int(parts[3]), 10)) if len(parts) >= 4 else 1
        except ValueError:
            log_warn("Usage: !tc <source_id> <target_id> [batch_size]")
            return

        try: await message.delete()
        except Exception: pass

        src = find_channel_by_id(source_id)
        dst = find_channel_by_id(target_id)

        if not src or not dst:
            log_error(f"Channel(s) not found — src:{source_id} dst:{target_id}")
            return

        log_info(f"Transfer: #{src.name} -> #{dst.name} (batch={batch_size})")
        file_batch = []
        total = 0

        try:
            async for msg in src.history(limit=None, oldest_first=True):
                for att in msg.attachments:
                    if att.size > MAX_FILE_SIZE:
                        log_warn(f"Skipped oversized file: {att.filename} ({att.size/1024/1024:.1f} MB)")
                        continue
                    try:
                        file_batch.append(await att.to_file())
                    except Exception as e:
                        log_error(f"Failed to download {att.filename}: {e}")
                        continue

                    if len(file_batch) >= batch_size:
                        try:
                            await dst.send(files=file_batch)
                            total += len(file_batch)
                            log_info(f"Sent {len(file_batch)} file(s) (total: {total})")
                        except discord.HTTPException as e:
                            log_error(f"HTTP error sending batch: {e.status} {e.text}")
                        except Exception as e:
                            log_error(f"Send error: {e}")
                        file_batch = []
                        await human_sleep(1.5, 4.0)

            if file_batch:
                try:
                    await dst.send(files=file_batch)
                    total += len(file_batch)
                except Exception as e:
                    log_error(f"Final batch send failed: {e}")

        except Exception as e:
            log_error(f"Error reading source channel history: {e}")
            log_error(traceback.format_exc())

        log_ok(f"Transfer complete. Total files sent: {total}")

    # ── !tm ───────────────────────────────────────────────────
    elif content.startswith("!tm"):
        parts = content.split()
        if len(parts) < 2:
            log_warn("Usage: !tm <channel_id> [batch_size]")
            return
        try:
            channel_id = int(parts[1])
            batch_size = int(parts[2]) if len(parts) >= 3 else 1
        except ValueError:
            log_warn("Usage: !tm <channel_id> [batch_size]")
            return

        try: await message.delete()
        except Exception: pass

        target = find_channel_by_id(channel_id)
        if not target:
            log_error(f"Channel {channel_id} not found.")
            return

        log_info(f"Uploading local media to #{target.name} (batch={batch_size})")
        await send_media_in_batches(target, batch_size)

    # ── !clone ────────────────────────────────────────────────
    elif content.startswith("!clones"):
        # Multi-clone: !clones <dst_cat_id> <src1> <src2> ...
        parts = content.split()
        if len(parts) < 3:
            log_warn("Usage: !clones <dst_category_id> <src_id1> <src_id2> ...")
            return

        try: await message.delete()
        except Exception: pass

        try:
            dst_id  = int(parts[1])
            src_ids = [int(x) for x in parts[2:]]
        except ValueError:
            log_warn("All IDs must be numbers.")
            return

        guild    = message.guild
        dst_cat  = guild.get_channel(dst_id) if guild else None

        if not dst_cat or not isinstance(dst_cat, discord.CategoryChannel):
            log_error(f"Destination category {dst_id} not found or not a category.")
            return

        log_info(f"Multi-clone: {len(src_ids)} source(s) -> '{dst_cat.name}'")

        for idx, src_id in enumerate(src_ids, 1):
            src_cat = find_category(src_id)
            if not src_cat:
                log_error(f"[{idx}/{len(src_ids)}] Source category {src_id} not found — skipping")
                continue
            log_info(f"[{idx}/{len(src_ids)}] Cloning '{src_cat.name}' ({src_id})")
            try:
                await clone_category(src_cat, dst_cat, guild)
            except Exception as e:
                log_error(f"[{idx}/{len(src_ids)}] Unhandled error cloning '{src_cat.name}': {e}")
                log_error(traceback.format_exc())
            await human_sleep(2.0, 5.0)

        log_ok(f"Multi-clone complete. All {len(src_ids)} source(s) processed.")

    elif content.startswith("!clone"):
        parts = content.split()
        if len(parts) != 3:
            log_warn("Usage: !clone <src_category_id> <dst_category_id>")
            return

        try: await message.delete()
        except Exception: pass

        try:
            src_id = int(parts[1])
            dst_id = int(parts[2])
        except ValueError:
            log_warn("Both IDs must be numbers.")
            return

        guild   = message.guild
        src_cat = find_category(src_id)
        dst_cat = guild.get_channel(dst_id) if guild else None

        if not src_cat:
            log_error(f"Source category {src_id} not found.")
            return
        if not dst_cat or not isinstance(dst_cat, discord.CategoryChannel):
            log_error(f"Destination category {dst_id} not found or not a category.")
            return

        try:
            await clone_category(src_cat, dst_cat, guild)
        except Exception as e:
            log_error(f"Unhandled error during clone: {e}")
            log_error(traceback.format_exc())

    # ── !kaboom ───────────────────────────────────────────────
    elif content.startswith("!kaboom"):
        parts = content.split()
        batch_size = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1

        try: await message.delete()
        except Exception: pass

        log_info(f"Kaboom! Uploading to current channel (batch={batch_size})")
        try:
            await send_media_in_batches(message.channel, batch_size)
        except Exception as e:
            log_error(f"Kaboom error: {e}")
            log_error(traceback.format_exc())

    # ── !bump ─────────────────────────────────────────────────
    elif content.startswith("!bump"):
        try: await message.delete()
        except Exception: pass

        channel = message.channel
        disboard_id = 302050872383242240
        log_info("Auto-bumper started.")

        while True:
            try:
                app_commands = await channel.application_commands()
                cmd = next(
                    (c for c in app_commands if c.name == "bump" and c.application_id == disboard_id),
                    None
                )
                if cmd:
                    await cmd()
                    log_ok(f"Bump executed.")
                else:
                    log_warn("/bump not found in this channel. Is Disboard here?")
            except discord.HTTPException as e:
                log_error(f"HTTP error during bump: {e.status} {e.text}")
            except Exception as e:
                log_error(f"Bump error: {e}")
                log_error(traceback.format_exc())

            wait = random.randint(6900, 7800)
            log_info(f"Next bump in {wait // 60}m {wait % 60}s")
            await asyncio.sleep(wait)


if __name__ == '__main__':
    initialize()
    TOKEN = getattr(config, 'TOKEN', None) or input("Discord profile token: ")
    bot.run(TOKEN)
