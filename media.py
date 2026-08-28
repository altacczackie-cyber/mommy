import datetime
import os
import shutil
import asyncio
import random
import traceback
import json
import discord
from discord.ext import commands
import config

LARGE_MEDIA    = ""
CONTENT_FOLDER = ""
LOG_FILE       = ""
TC_STATE_FILE  = "./tc_state.json"

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB
MAX_RETRIES   = 5

bot = commands.Bot(command_prefix='!', self_bot=True)

# ── LOGGING ───────────────────────────────────────────────────

def ts():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def log(level, msg):
    print(f"[{ts()}] [{level}] {msg}", flush=True)

def log_info(msg):  log("INFO",  msg)
def log_warn(msg):  log("WARN",  msg)
def log_error(msg): log("ERROR", msg)
def log_ok(msg):    log("OK",    msg)

# ── ANTI-DETECTION ────────────────────────────────────────────

def night_multiplier():
    hour = datetime.datetime.now(datetime.timezone.utc).hour
    if 2 <= hour < 8:
        return random.uniform(2.5, 5.0)
    return 1.0

async def human_sleep(min_s=1.0, max_s=3.5):
    await asyncio.sleep(random.uniform(min_s, max_s) * night_multiplier())

# ── TC CHECKPOINT (resume after restart) ──────────────────────

def save_tc_state(source_id, target_id, batch_size, last_message_id, transferred):
    state = {
        "source_id":      source_id,
        "target_id":      target_id,
        "batch_size":     batch_size,
        "last_message_id": last_message_id,
        "transferred":    transferred,
    }
    with open(TC_STATE_FILE, "w") as f:
        json.dump(state, f)

def load_tc_state():
    if os.path.exists(TC_STATE_FILE):
        try:
            with open(TC_STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return None

def clear_tc_state():
    if os.path.exists(TC_STATE_FILE):
        os.remove(TC_STATE_FILE)

# ── RETRY HELPERS ─────────────────────────────────────────────

async def safe_send(channel, *, content=None, files=None, attempt=0):
    try:
        return await channel.send(content=content, files=files or [])
    except discord.HTTPException as e:
        if e.status == 429:
            retry_after = 5 + random.uniform(0.5, 2.0)
            try:
                retry_after = float(e.response.headers.get("Retry-After", 5)) + random.uniform(0.5, 2.0)
            except Exception:
                pass
            log_warn(f"Rate limited (429). Waiting {retry_after:.1f}s (attempt {attempt+1}/{MAX_RETRIES})...")
            await asyncio.sleep(retry_after)
            if attempt < MAX_RETRIES:
                return await safe_send(channel, content=content, files=files, attempt=attempt + 1)
            log_error("Max retries on rate limit — skipping.")
            return None
        elif e.status in (500, 502, 503, 504):
            wait = (2 ** attempt) + random.uniform(0, 1)
            log_warn(f"Server error {e.status}. Retrying in {wait:.1f}s...")
            await asyncio.sleep(wait)
            if attempt < MAX_RETRIES:
                return await safe_send(channel, content=content, files=files, attempt=attempt + 1)
            log_error("Max retries on server error — skipping.")
            return None
        else:
            log_error(f"HTTP {e.status}: {e.text}")
            return None
    except (asyncio.TimeoutError, discord.ConnectionClosed) as e:
        wait = (2 ** attempt) + random.uniform(0, 1)
        log_warn(f"Connection issue: {type(e).__name__}. Retrying in {wait:.1f}s...")
        await asyncio.sleep(wait)
        if attempt < MAX_RETRIES:
            return await safe_send(channel, content=content, files=files, attempt=attempt + 1)
        log_error("Max retries on connection error — skipping.")
        return None
    except Exception as e:
        log_error(f"Unexpected error in safe_send: {e}")
        log_error(traceback.format_exc())
        return None

async def safe_fetch_file(attachment, attempt=0):
    try:
        return await attachment.to_file()
    except discord.HTTPException as e:
        if e.status == 429:
            retry_after = 5 + random.uniform(0.5, 2.0)
            log_warn(f"Rate limited downloading {attachment.filename}. Waiting {retry_after:.1f}s...")
            await asyncio.sleep(retry_after)
            if attempt < MAX_RETRIES:
                return await safe_fetch_file(attachment, attempt + 1)
        elif e.status in (403, 404):
            log_warn(f"Attachment {attachment.filename} unavailable ({e.status}) — skipping")
            return None
        else:
            log_warn(f"HTTP {e.status} downloading {attachment.filename}: {e.text}")
        return None
    except (asyncio.TimeoutError, Exception) as e:
        log_warn(f"Error downloading {attachment.filename}: {type(e).__name__}: {e}")
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)
            return await safe_fetch_file(attachment, attempt + 1)
        return None

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
    total    = len(all_files)
    uploaded = 0
    failed   = 0

    if total == 0:
        log_info("No new files to upload.")
        return

    log_info(f"Starting upload of {total} file(s) in batches of {batch_size}")

    for i in range(0, total, batch_size):
        batch = all_files[i:i + batch_size]
        names = ", ".join(f[1] for f in batch)
        log_info(f"Batch [{i+1}-{min(i+batch_size, total)}/{total}]: {names}")

        discord_files = []
        for fpath, fname, _ in batch:
            try:
                discord_files.append(discord.File(fpath, fname))
            except Exception as e:
                log_error(f"Could not open {fname}: {e}")
                failed += 1

        if not discord_files:
            continue

        result = await safe_send(channel, files=discord_files)
        if result:
            now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            with open(LOG_FILE, 'a') as f:
                for _, fname, size_mb in batch:
                    f.write(f"{now}  {size_mb:8.2f}    {fname}\n")
            uploaded += len(discord_files)
            log_ok(f"Sent {len(discord_files)} file(s) (total: {uploaded})")
        else:
            failed += len(discord_files)
            for fpath, fname, _ in batch:
                if os.path.dirname(fpath) == CONTENT_FOLDER:
                    shutil.move(fpath, os.path.join(LARGE_MEDIA, fname))

        await human_sleep(1.2, 4.0)
        if random.random() < 0.12:
            pause = random.uniform(5, 15)
            log_info(f"Short break {pause:.1f}s...")
            await asyncio.sleep(pause)

    log_ok(f"Upload done — uploaded: {uploaded} | failed: {failed} | total: {total}")

# ── TC WITH RESUME ────────────────────────────────────────────

async def run_tc(src, dst, batch_size, resume_after_id=None, already_transferred=0):
    source_id = src.id
    target_id = dst.id
    file_batch = []
    total      = already_transferred
    skipped    = 0
    last_msg_id = resume_after_id

    log_info(f"Transfer: #{src.name} -> #{dst.name} (batch={batch_size})" +
             (f" | Resuming after message {resume_after_id}" if resume_after_id else ""))

    try:
        kwargs = {"limit": None, "oldest_first": True}
        if resume_after_id:
            kwargs["after"] = discord.Object(id=resume_after_id)

        async for msg in src.history(**kwargs):
            for att in msg.attachments:
                if att.size > MAX_FILE_SIZE:
                    log_warn(f"Skipped oversized {att.filename} ({att.size/1024/1024:.1f} MB)")
                    skipped += 1
                    continue

                f = await safe_fetch_file(att)
                if f:
                    file_batch.append(f)
                else:
                    skipped += 1

                if len(file_batch) >= batch_size:
                    result = await safe_send(dst, files=file_batch)
                    if result:
                        total += len(file_batch)
                        log_info(f"Sent {len(file_batch)} file(s) (total: {total})")
                    file_batch = []
                    # Save checkpoint after every successful batch
                    last_msg_id = msg.id
                    save_tc_state(source_id, target_id, batch_size, last_msg_id, total)
                    await human_sleep(1.5, 4.0)

            last_msg_id = msg.id

        # Send remaining files
        if file_batch:
            result = await safe_send(dst, files=file_batch)
            if result:
                total += len(file_batch)
            file_batch = []

    except discord.Forbidden:
        log_error(f"No permission to read #{src.name}")
    except discord.HTTPException as e:
        log_error(f"HTTP {e.status} reading history: {e.text}")
        save_tc_state(source_id, target_id, batch_size, last_msg_id, total)
        log_warn("State saved — will resume from here on next restart.")
        return
    except Exception as e:
        log_error(f"Unexpected error during transfer: {e}")
        log_error(traceback.format_exc())
        save_tc_state(source_id, target_id, batch_size, last_msg_id, total)
        log_warn("State saved — will resume from here on next restart.")
        return

    # Done — clear checkpoint
    clear_tc_state()
    log_ok(f"Transfer complete — total: {total} | skipped: {skipped}")

# ── CLONE ─────────────────────────────────────────────────────

async def clone_category(source_cat, target_cat, guild):
    log_info(f"Cloning '{source_cat.name}' ({source_cat.id}) -> '{target_cat.name}' ({target_cat.id})")

    existing  = {c.name.lower() for c in target_cat.text_channels}
    channels  = list(source_cat.text_channels)
    cloned    = 0
    skipped   = 0
    msgs_ok   = 0
    msgs_err  = 0

    log_info(f"Found {len(channels)} text channel(s)")

    for ch in channels:
        if ch.name.lower() in existing:
            log_warn(f"Skipping #{ch.name} — already exists")
            skipped += 1
            continue

        new_ch = None
        for attempt in range(MAX_RETRIES):
            try:
                new_ch = await guild.create_text_channel(ch.name, category=target_cat, nsfw=True)
                log_info(f"Created #{new_ch.name}")
                cloned += 1
                break
            except discord.HTTPException as e:
                if e.status == 429:
                    wait = 5 + random.uniform(0.5, 2)
                    log_warn(f"Rate limited creating #{ch.name}. Waiting {wait:.1f}s...")
                    await asyncio.sleep(wait)
                elif e.status == 403:
                    log_error(f"No permission to create #{ch.name} — skipping")
                    skipped += 1
                    break
                else:
                    log_error(f"HTTP {e.status} creating #{ch.name}: {e.text}")
                    await asyncio.sleep(2 ** attempt)
                    if attempt == MAX_RETRIES - 1:
                        skipped += 1
            except Exception as e:
                log_error(f"Error creating #{ch.name}: {e}")
                skipped += 1
                break

        if not new_ch:
            continue

        log_info(f"Copying messages from #{ch.name}...")
        msg_count = 0

        try:
            async for m in ch.history(limit=None, oldest_first=True):
                content = m.content or ""
                files   = []

                for att in m.attachments:
                    if att.size > MAX_FILE_SIZE:
                        log_warn(f"Skipped oversized attachment {att.filename}")
                        continue
                    f = await safe_fetch_file(att)
                    if f:
                        files.append(f)

                if files:
                    for j in range(0, len(files), 10):
                        chunk   = files[j:j + 10]
                        caption = f"**{m.author.name}**: {content}" if content and j == 0 else None
                        result  = await safe_send(new_ch, content=caption, files=chunk)
                        if result:
                            msgs_ok += 1
                        else:
                            msgs_err += 1
                        await human_sleep(0.8, 2.0)
                elif content.strip():
                    result = await safe_send(new_ch, content=f"**{m.author.name}**: {content}")
                    if result:
                        msgs_ok += 1
                    else:
                        msgs_err += 1
                    await human_sleep(0.5, 1.5)

                msg_count += 1
                if msg_count % 50 == 0:
                    log_info(f"  #{ch.name}: {msg_count} messages processed...")

        except discord.Forbidden:
            log_error(f"No read permission for #{ch.name}")
        except discord.HTTPException as e:
            log_error(f"HTTP {e.status} reading #{ch.name}: {e.text}")
        except Exception as e:
            log_error(f"Error reading #{ch.name}: {e}")
            log_error(traceback.format_exc())

        log_ok(f"#{ch.name} done — {msg_count} messages | sent: {msgs_ok} | errors: {msgs_err}")
        await human_sleep(1.5, 4.0)

    log_ok(
        f"Category '{source_cat.name}' done — "
        f"channels cloned: {cloned} | skipped: {skipped} | "
        f"messages sent: {msgs_ok} | errors: {msgs_err}"
    )

# ── EVENTS ────────────────────────────────────────────────────

@bot.event
async def on_ready():
    log_ok(f"Logged in as {bot.user} ({bot.user.id})")
    log_info("Commands:")
    log_info("  !tm <channel_id> [batch]                   Upload media/ to a channel")
    log_info("  !tc <src_id> <dst_id> [batch]              Transfer files channel->channel (auto-resume)")
    log_info("  !kaboom [batch]                            Upload media/ to current channel")
    log_info("  !clone <src_cat_id> <dst_cat_id>           Clone one category")
    log_info("  !clones <dst_cat_id> <src1> <src2> ...     Clone multiple categories into one")
    log_info("  !bump                                      Start auto-bumper")
    log_info("  !tcreset                                   Clear pending TC resume state")

    # Auto-resume TC if interrupted
    state = load_tc_state()
    if state:
        log_warn(
            f"Found incomplete TC transfer: "
            f"src={state['source_id']} dst={state['target_id']} "
            f"batch={state['batch_size']} already_sent={state['transferred']} "
            f"last_msg={state['last_message_id']}"
        )
        log_info("Auto-resuming in 5 seconds... (send !tcreset to cancel)")
        await asyncio.sleep(5)

        # Re-check state (user might have sent !tcreset)
        state = load_tc_state()
        if not state:
            log_info("TC resume cancelled.")
            return

        src = find_channel_by_id(state["source_id"])
        dst = find_channel_by_id(state["target_id"])

        if not src or not dst:
            log_error("TC resume failed — channel(s) not found. Clearing state.")
            clear_tc_state()
            return

        log_info(f"Resuming TC: #{src.name} -> #{dst.name}")
        await run_tc(
            src, dst,
            state["batch_size"],
            resume_after_id=state["last_message_id"],
            already_transferred=state["transferred"],
        )

@bot.event
async def on_error(event, *args, **kwargs):
    log_error(f"Unhandled error in event '{event}':")
    log_error(traceback.format_exc())

@bot.event
async def on_message(message):
    if message.author != bot.user:
        return

    content = message.content.strip()

    # ── !tcreset ──────────────────────────────────────────────
    if content == "!tcreset":
        clear_tc_state()
        log_ok("TC resume state cleared.")
        try: await message.delete()
        except Exception: pass

    # ── !tc ───────────────────────────────────────────────────
    elif content.startswith("!tc"):
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

        # Save initial state immediately
        save_tc_state(source_id, target_id, batch_size, None, 0)
        await run_tc(src, dst, batch_size)

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

    # ── !clones ───────────────────────────────────────────────
    elif content.startswith("!clones"):
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

        guild   = message.guild
        dst_cat = guild.get_channel(dst_id) if guild else None

        if not dst_cat or not isinstance(dst_cat, discord.CategoryChannel):
            log_error(f"Destination category {dst_id} not found.")
            return

        log_info(f"Multi-clone: {len(src_ids)} source(s) -> '{dst_cat.name}'")
        for idx, src_id in enumerate(src_ids, 1):
            src_cat = find_category(src_id)
            if not src_cat:
                log_error(f"[{idx}/{len(src_ids)}] Source {src_id} not found — skipping")
                continue
            log_info(f"[{idx}/{len(src_ids)}] Cloning '{src_cat.name}'")
            try:
                await clone_category(src_cat, dst_cat, guild)
            except Exception as e:
                log_error(f"[{idx}/{len(src_ids)}] Error: {e}")
                log_error(traceback.format_exc())
            await human_sleep(2.0, 5.0)

        log_ok(f"Multi-clone complete — {len(src_ids)} source(s) processed.")

    # ── !clone ────────────────────────────────────────────────
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
            log_error(f"Destination category {dst_id} not found.")
            return

        try:
            await clone_category(src_cat, dst_cat, guild)
        except Exception as e:
            log_error(f"Error during clone: {e}")
            log_error(traceback.format_exc())

    # ── !kaboom ───────────────────────────────────────────────
    elif content.startswith("!kaboom"):
        parts = content.split()
        batch_size = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1

        try: await message.delete()
        except Exception: pass

        log_info(f"Kaboom — uploading to current channel (batch={batch_size})")
        try:
            await send_media_in_batches(message.channel, batch_size)
        except Exception as e:
            log_error(f"Kaboom error: {e}")
            log_error(traceback.format_exc())

    # ── !bump ─────────────────────────────────────────────────
    elif content.startswith("!bump"):
        try: await message.delete()
        except Exception: pass

        channel     = message.channel
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
                    log_ok("Bump executed.")
                else:
                    log_warn("/bump not found — is Disboard in this channel?")
            except discord.HTTPException as e:
                log_error(f"HTTP {e.status} during bump: {e.text}")
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
