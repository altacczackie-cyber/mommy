import datetime
import os
import shutil
import asyncio
import random
import traceback
import json
import tempfile
import aiohttp
import discord
from discord.ext import commands
import config

LARGE_MEDIA    = ""
CONTENT_FOLDER = ""
LOG_FILE       = ""
TC_STATE_FILE  = "./tc_state.json"
TMS_STATE_FILE = "./tms_state.json"
SNIPE_STATE_FILE = "./snipe_state.json"

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB
MAX_RETRIES   = 5

bot = commands.Bot(command_prefix='!', self_bot=True)

# Track whether on_ready has already run (avoid duplicate tasks on reconnect)
_ready_done      = False
snipe_task       = None
_watchdog_task   = None

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

# ── STATE ─────────────────────────────────────────────────────

def save_tc_state(source_id, target_id, batch_size, last_message_id, transferred, fail_count=0):
    with open(TC_STATE_FILE, "w") as f:
        json.dump({
            "source_id": source_id, "target_id": target_id,
            "batch_size": batch_size, "last_message_id": last_message_id,
            "transferred": transferred, "fail_count": fail_count,
        }, f)

def load_tc_state():
    if os.path.exists(TC_STATE_FILE):
        try:
            with open(TC_STATE_FILE) as f: return json.load(f)
        except Exception: pass
    return None

def clear_tc_state():
    if os.path.exists(TC_STATE_FILE): os.remove(TC_STATE_FILE)

def save_tms_state(target_guild_id, src_ids, channel_index, last_message_id, transferred, fail_count=0):
    with open(TMS_STATE_FILE, "w") as f:
        json.dump({
            "target_guild_id": target_guild_id, "src_ids": src_ids,
            "channel_index": channel_index, "last_message_id": last_message_id,
            "transferred": transferred, "fail_count": fail_count,
        }, f)

def load_tms_state():
    if os.path.exists(TMS_STATE_FILE):
        try:
            with open(TMS_STATE_FILE) as f: return json.load(f)
        except Exception: pass
    return None

def clear_tms_state():
    if os.path.exists(TMS_STATE_FILE): os.remove(TMS_STATE_FILE)

# ── STREAMING DOWNLOAD ────────────────────────────────────────
# Downloads to disk instead of memory — prevents OOM kills on large videos

async def stream_to_disk(url: str, filename: str, attempt=0):
    """Download a URL to a temp file on disk. Returns (temp_path, discord.File) or None."""
    suffix = os.path.splitext(filename)[1] or ".bin"
    tmp_path = None
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp_path = tmp.name
        tmp.close()

        timeout = aiohttp.ClientTimeout(total=300, connect=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    with open(tmp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(1024 * 512):
                            f.write(chunk)
                    return tmp_path, discord.File(tmp_path, filename=filename)
                elif resp.status in (403, 404):
                    log_warn(f"Attachment URL expired or unavailable ({resp.status}): {filename}")
                    return None, None
                else:
                    log_warn(f"HTTP {resp.status} downloading {filename}")
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(2 ** attempt)
                        return await stream_to_disk(url, filename, attempt + 1)
                    return None, None
    except asyncio.TimeoutError:
        log_warn(f"Timeout downloading {filename} (attempt {attempt+1})")
        if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(5 * (attempt + 1))
            return await stream_to_disk(url, filename, attempt + 1)
        return None, None
    except Exception as e:
        log_warn(f"Error downloading {filename}: {e}")
        if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)
            return await stream_to_disk(url, filename, attempt + 1)
        return None, None

def cleanup_tmp(*paths):
    for p in paths:
        if p and os.path.exists(p):
            try: os.unlink(p)
            except Exception: pass

# ── SAFE SEND ─────────────────────────────────────────────────

async def safe_send(channel, *, content=None, files=None, attempt=0):
    try:
        return await channel.send(content=content, files=files or [])
    except discord.HTTPException as e:
        if e.status == 429:
            wait = 5 + random.uniform(0.5, 2.0)
            try: wait = float(e.response.headers.get("Retry-After", 5)) + random.uniform(0.5, 2.0)
            except Exception: pass
            log_warn(f"Rate limited (429) — waiting {wait:.1f}s (attempt {attempt+1})")
            await asyncio.sleep(wait)
            if attempt < MAX_RETRIES:
                return await safe_send(channel, content=content, files=files, attempt=attempt+1)
            log_error("Max retries on rate limit — skipping batch")
            return None
        elif e.status in (500, 502, 503, 504):
            wait = (2 ** attempt) + random.uniform(0, 1)
            log_warn(f"Discord server error {e.status} — retrying in {wait:.1f}s")
            await asyncio.sleep(wait)
            if attempt < MAX_RETRIES:
                return await safe_send(channel, content=content, files=files, attempt=attempt+1)
            return None
        else:
            log_error(f"HTTP {e.status} sending: {e.text}")
            return None
    except (asyncio.TimeoutError, discord.ConnectionClosed) as e:
        wait = (2 ** attempt) + random.uniform(0, 1)
        log_warn(f"{type(e).__name__} sending — retrying in {wait:.1f}s")
        await asyncio.sleep(wait)
        if attempt < MAX_RETRIES:
            return await safe_send(channel, content=content, files=files, attempt=attempt+1)
        return None
    except Exception as e:
        log_error(f"Unexpected send error: {e}")
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
    if ch: return ch
    for guild in bot.guilds:
        ch = guild.get_channel(channel_id)
        if ch: return ch
    return None

def find_category(category_id: int):
    for guild in bot.guilds:
        ch = guild.get_channel(category_id)
        if isinstance(ch, discord.CategoryChannel): return ch
    return None

# ── LOCAL UPLOAD ──────────────────────────────────────────────

async def send_media_in_batches(channel, batch_size=1):
    batch_size = max(1, min(batch_size, 10))
    with open(LOG_FILE, 'r') as f:
        already_uploaded = f.read()

    all_files = []
    for folder in [CONTENT_FOLDER, LARGE_MEDIA]:
        for fname in os.listdir(folder):
            fpath = os.path.join(folder, fname)
            if not os.path.isfile(fpath) or fname in already_uploaded: continue
            size_bytes = os.path.getsize(fpath)
            size_mb = size_bytes / (1024 * 1024)
            if size_bytes > MAX_FILE_SIZE:
                log_warn(f"Skipped {fname} ({size_mb:.1f} MB) — exceeds 500 MB limit")
                if folder == CONTENT_FOLDER: shutil.move(fpath, os.path.join(LARGE_MEDIA, fname))
                continue
            all_files.append((fpath, fname, size_mb))

    random.shuffle(all_files)
    total, uploaded, failed = len(all_files), 0, 0
    if total == 0:
        log_info("No new files to upload.")
        return
    log_info(f"Starting upload of {total} file(s) in batches of {batch_size}")

    for i in range(0, total, batch_size):
        batch = all_files[i:i + batch_size]
        names = ", ".join(f[1] for f in batch)
        log_info(f"Batch [{i+1}-{min(i+batch_size, total)}/{total}]: {names}")
        dfiles = []
        for fpath, fname, _ in batch:
            try: dfiles.append(discord.File(fpath, fname))
            except Exception as e:
                log_error(f"Could not open {fname}: {e}")
                failed += 1
        if not dfiles: continue
        result = await safe_send(channel, files=dfiles)
        if result:
            now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            with open(LOG_FILE, 'a') as f:
                for _, fname, size_mb in batch:
                    f.write(f"{now}  {size_mb:8.2f}    {fname}\n")
            uploaded += len(dfiles)
            log_ok(f"Sent {len(dfiles)} file(s) (total: {uploaded})")
        else:
            failed += len(dfiles)
            for fpath, fname, _ in batch:
                if os.path.dirname(fpath) == CONTENT_FOLDER:
                    shutil.move(fpath, os.path.join(LARGE_MEDIA, fname))
        await human_sleep(1.2, 4.0)
        if random.random() < 0.12:
            pause = random.uniform(5, 15)
            log_info(f"Short break {pause:.1f}s...")
            await asyncio.sleep(pause)

    log_ok(f"Upload done — uploaded: {uploaded} | failed: {failed} | total: {total}")

# ── TC: CHANNEL TRANSFER ──────────────────────────────────────

async def run_tc(src, dst, batch_size, resume_after_id=None, already_transferred=0, fail_count=0):
    source_id, target_id = src.id, dst.id
    file_batch, tmp_paths = [], []
    total, skipped = already_transferred, 0
    last_msg_id = resume_after_id

    log_info(f"Transfer: #{src.name} -> #{dst.name} (batch={batch_size})" +
             (f" | Resuming after {resume_after_id}" if resume_after_id else ""))

    try:
        kwargs = {"limit": None, "oldest_first": True}
        if resume_after_id:
            kwargs["after"] = discord.Object(id=resume_after_id)

        async for msg in src.history(**kwargs):
            msg_files, msg_tmps = [], []

            for att in msg.attachments:
                if att.size > MAX_FILE_SIZE:
                    log_warn(f"Skipped oversized {att.filename} ({att.size/1024/1024:.1f} MB)")
                    skipped += 1
                    continue
                log_info(f"Downloading {att.filename} ({att.size/1024/1024:.1f} MB)...")
                tmp_path, dfile = await stream_to_disk(att.url, att.filename)
                if dfile:
                    msg_files.append(dfile)
                    msg_tmps.append(tmp_path)
                else:
                    skipped += 1

            file_batch.extend(msg_files)
            tmp_paths.extend(msg_tmps)

            while len(file_batch) >= batch_size:
                chunk = file_batch[:batch_size]
                chunk_tmps = tmp_paths[:batch_size]
                file_batch = file_batch[batch_size:]
                tmp_paths = tmp_paths[batch_size:]
                result = await safe_send(dst, files=chunk)
                cleanup_tmp(*chunk_tmps)
                if result:
                    total += len(chunk)
                    log_info(f"Sent {len(chunk)} file(s) (total: {total})")
                await human_sleep(1.5, 4.0)

            # Checkpoint after each full message — no re-raise, just save
            last_msg_id = msg.id
            save_tc_state(source_id, target_id, batch_size, last_msg_id, total, 0)

        if file_batch:
            result = await safe_send(dst, files=file_batch)
            cleanup_tmp(*tmp_paths)
            if result: total += len(file_batch)

    except discord.Forbidden:
        log_error("No permission to read source channel — clearing state")
        cleanup_tmp(*tmp_paths)
        clear_tc_state()
        return
    except discord.HTTPException as e:
        log_error(f"HTTP {e.status}: {e.text}")
        cleanup_tmp(*tmp_paths)
        save_tc_state(source_id, target_id, batch_size, last_msg_id, total, 0)
        log_warn("Checkpoint saved.")
        return
    except (asyncio.CancelledError, GeneratorExit, Exception) as e:
        # NEVER re-raise — save and return cleanly so Railway doesn't loop
        if not isinstance(e, Exception):
            log_warn(f"{type(e).__name__} caught — saving checkpoint, exiting cleanly")
        else:
            log_error(f"Error during transfer: {e}")
            log_error(traceback.format_exc())
        cleanup_tmp(*tmp_paths)
        save_tc_state(source_id, target_id, batch_size, last_msg_id, total, 0)
        return

    clear_tc_state()
    log_ok(f"Transfer complete — total: {total} | skipped: {skipped}")

# ── TMS: MULTI SOURCE ─────────────────────────────────────────

async def run_tms(target_guild, src_ids, start_channel_index=0, resume_after_msg_id=None, already_transferred=0):
    src_ids_sorted = sorted(src_ids)
    total_channels = len(src_ids_sorted)
    total_transferred = already_transferred

    log_info(f"TMS: {total_channels} channel(s) -> guild '{target_guild.name}'")

    for ch_idx in range(start_channel_index, total_channels):
        src_id = src_ids_sorted[ch_idx]
        src = find_channel_by_id(src_id)

        if not src:
            log_error(f"[{ch_idx+1}/{total_channels}] Source {src_id} not found — skipping")
            save_tms_state(target_guild.id, src_ids, ch_idx + 1, None, total_transferred, 0)
            continue

        log_info(f"[{ch_idx+1}/{total_channels}] Processing #{src.name} ({src_id})")

        dst = discord.utils.get(target_guild.text_channels, name=src.name)
        if dst:
            log_warn(f"#{src.name} already exists in target — using it")
        else:
            dst = None
            for attempt in range(MAX_RETRIES):
                try:
                    dst = await target_guild.create_text_channel(src.name, nsfw=True)
                    log_ok(f"Created #{dst.name}")
                    break
                except discord.HTTPException as e:
                    if e.status == 429:
                        wait = 5 + random.uniform(1, 3)
                        log_warn(f"Rate limited — waiting {wait:.1f}s...")
                        await asyncio.sleep(wait)
                    elif e.status == 403:
                        log_error(f"No permission to create channels — aborting TMS")
                        clear_tms_state()
                        return
                    else:
                        log_error(f"HTTP {e.status} creating #{src.name}: {e.text}")
                        await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    log_error(f"Error creating channel: {e}")
                    break

            if not dst:
                log_error(f"Could not create #{src.name} — skipping")
                save_tms_state(target_guild.id, src_ids, ch_idx + 1, None, total_transferred, 0)
                continue

        log_info(f"Transferring #{src.name} -> #{dst.name}" +
                 (f" (resume after {resume_after_msg_id})" if ch_idx == start_channel_index and resume_after_msg_id else ""))

        file_batch, tmp_paths = [], []
        last_msg_id = resume_after_msg_id if ch_idx == start_channel_index else None
        ch_transferred, ch_skipped = 0, 0

        try:
            kwargs = {"limit": None, "oldest_first": True}
            if last_msg_id:
                kwargs["after"] = discord.Object(id=last_msg_id)

            async for msg in src.history(**kwargs):
                msg_files, msg_tmps = [], []

                for att in msg.attachments:
                    if att.size > MAX_FILE_SIZE:
                        log_warn(f"Skipped oversized {att.filename}")
                        ch_skipped += 1
                        continue
                    log_info(f"  Downloading {att.filename} ({att.size/1024/1024:.1f} MB)...")
                    tmp_path, dfile = await stream_to_disk(att.url, att.filename)
                    if dfile:
                        msg_files.append(dfile)
                        msg_tmps.append(tmp_path)
                    else:
                        ch_skipped += 1

                file_batch.extend(msg_files)
                tmp_paths.extend(msg_tmps)

                while len(file_batch) >= 5:
                    chunk = file_batch[:5]
                    chunk_tmps = tmp_paths[:5]
                    file_batch = file_batch[5:]
                    tmp_paths = tmp_paths[5:]
                    result = await safe_send(dst, files=chunk)
                    cleanup_tmp(*chunk_tmps)
                    if result:
                        ch_transferred += len(chunk)
                        total_transferred += len(chunk)
                        log_info(f"  [{ch_idx+1}/{total_channels}] #{src.name}: sent (ch: {ch_transferred})")
                    await human_sleep(1.5, 4.0)

                last_msg_id = msg.id
                save_tms_state(target_guild.id, src_ids, ch_idx, last_msg_id, total_transferred, 0)

            if file_batch:
                result = await safe_send(dst, files=file_batch)
                cleanup_tmp(*tmp_paths)
                if result:
                    ch_transferred += len(file_batch)
                    total_transferred += len(file_batch)
                file_batch, tmp_paths = [], []

        except discord.Forbidden:
            log_error(f"No read permission for #{src.name} — skipping")
            cleanup_tmp(*tmp_paths)
            save_tms_state(target_guild.id, src_ids, ch_idx + 1, None, total_transferred, 0)
            resume_after_msg_id = None
            await human_sleep(2.0, 5.0)
            continue
        except (asyncio.CancelledError, GeneratorExit, Exception) as e:
            # Never re-raise — save checkpoint and return cleanly
            if not isinstance(e, Exception):
                log_warn(f"{type(e).__name__} caught in TMS — saving checkpoint")
            else:
                log_error(f"TMS error on #{src.name}: {e}")
                log_error(traceback.format_exc())
            cleanup_tmp(*tmp_paths)
            save_tms_state(target_guild.id, src_ids, ch_idx, last_msg_id, total_transferred, 0)
            return

        log_ok(f"[{ch_idx+1}/{total_channels}] #{src.name} done — sent: {ch_transferred} | skipped: {ch_skipped}")
        resume_after_msg_id = None
        save_tms_state(target_guild.id, src_ids, ch_idx + 1, None, total_transferred, 0)
        await human_sleep(2.0, 5.0)

    clear_tms_state()
    log_ok(f"TMS complete — total transferred: {total_transferred}")

# ── CLONE ─────────────────────────────────────────────────────

async def clone_category(source_cat, target_cat, guild):
    log_info(f"Cloning '{source_cat.name}' -> '{target_cat.name}'")
    existing = {c.name.lower() for c in target_cat.text_channels}
    channels = list(source_cat.text_channels)
    cloned, skipped, msgs_ok, msgs_err = 0, 0, 0, 0
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
                    log_warn(f"Rate limited — waiting {wait:.1f}s")
                    await asyncio.sleep(wait)
                elif e.status == 403:
                    log_error(f"No permission — skipping #{ch.name}")
                    skipped += 1
                    break
                else:
                    log_error(f"HTTP {e.status} creating #{ch.name}: {e.text}")
                    await asyncio.sleep(2 ** attempt)
                    if attempt == MAX_RETRIES - 1: skipped += 1
            except Exception as e:
                log_error(f"Error creating #{ch.name}: {e}")
                skipped += 1
                break

        if not new_ch: continue

        log_info(f"Copying messages from #{ch.name}...")
        msg_count = 0
        tmp_paths = []

        try:
            async for m in ch.history(limit=None, oldest_first=True):
                content = m.content or ""
                files, tmps = [], []

                for att in m.attachments:
                    if att.size > MAX_FILE_SIZE:
                        log_warn(f"Skipped oversized {att.filename}")
                        continue
                    tmp_path, dfile = await stream_to_disk(att.url, att.filename)
                    if dfile:
                        files.append(dfile)
                        tmps.append(tmp_path)

                if files:
                    for j in range(0, len(files), 10):
                        chunk = files[j:j+10]
                        chunk_tmps = tmps[j:j+10]
                        caption = f"**{m.author.name}**: {content}" if content and j == 0 else None
                        result = await safe_send(new_ch, content=caption, files=chunk)
                        cleanup_tmp(*chunk_tmps)
                        if result: msgs_ok += 1
                        else: msgs_err += 1
                        await human_sleep(0.8, 2.0)
                elif content.strip():
                    result = await safe_send(new_ch, content=f"**{m.author.name}**: {content}")
                    if result: msgs_ok += 1
                    else: msgs_err += 1
                    await human_sleep(0.5, 1.5)

                msg_count += 1
                if msg_count % 50 == 0:
                    log_info(f"  #{ch.name}: {msg_count} messages processed...")

        except discord.Forbidden:
            log_error(f"No read permission for #{ch.name}")
        except (asyncio.CancelledError, GeneratorExit, Exception) as e:
            if not isinstance(e, Exception):
                log_warn(f"{type(e).__name__} in clone — continuing to next channel")
            else:
                log_error(f"Error in #{ch.name}: {e}")
                log_error(traceback.format_exc())
            cleanup_tmp(*tmp_paths)

        log_ok(f"#{ch.name} done — {msg_count} messages | sent: {msgs_ok} | errors: {msgs_err}")
        await human_sleep(1.5, 4.0)

    log_ok(f"Category done — cloned: {cloned} | skipped: {skipped} | msgs: {msgs_ok}")


# ── VANITY SNIPER ─────────────────────────────────────────────

def save_snipe_state(vanity, guild_id):
    with open(SNIPE_STATE_FILE, "w") as f:
        json.dump({"vanity": vanity, "guild_id": guild_id}, f)

def load_snipe_state():
    if os.path.exists(SNIPE_STATE_FILE):
        try:
            with open(SNIPE_STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return None

def clear_snipe_state():
    if os.path.exists(SNIPE_STATE_FILE):
        os.remove(SNIPE_STATE_FILE)

async def check_vanity_free(vanity: str) -> bool:
    """Returns True if vanity is available. Uses bot.http for correct headers."""
    try:
        from discord.http import Route
        data = await bot.http.request(Route("GET", f"/invites/{vanity}"))
        taken = "guild" in data
        return not taken
    except discord.NotFound:
        return True   # 404 = definitely free
    except discord.HTTPException as e:
        if e.status == 429:
            retry = getattr(e, "retry_after", 1.0)
            log_warn(f"[SNIPER] Rate limited on check — waiting {retry:.1f}s")
            await asyncio.sleep(retry)
        return False
    except Exception as e:
        log_warn(f"[SNIPER] Check error: {type(e).__name__}: {e}")
        return False

class CloudflareBanError(Exception):
    def __init__(self, retry_after): self.retry_after = retry_after

# ── CAPTCHA SOLVER (optional) ────────────────────────────────
# Set CAPSOLVER_KEY env var to enable automatic captcha solving
# Get a key at capsolver.com (~$3/1000 solves)

async def solve_captcha(sitekey: str, rqdata: str = None) -> str | None:
    """Solve Discord hCaptcha via CapSolver. Returns token or None."""
    api_key = os.environ.get("CAPSOLVER_KEY", "").strip()
    if not api_key:
        log_error("[CAPTCHA] CAPTCHA required but CAPSOLVER_KEY not set — cannot solve automatically.")
        log_error("[CAPTCHA] Get a key at capsolver.com and add CAPSOLVER_KEY to Railway env vars.")
        return None

    try:
        log_warn("[CAPTCHA] Solving captcha via CapSolver...")
        payload = {
            "clientKey": api_key,
            "task": {
                "type":       "HCaptchaTaskProxyLess",
                "websiteURL": "https://discord.com",
                "websiteKey": sitekey,
            }
        }
        if rqdata:
            payload["task"]["enterprisePayload"] = {"rqdata": rqdata}

        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Create task
            async with session.post("https://api.capsolver.com/createTask", json=payload) as r:
                data = await r.json()
                task_id = data.get("taskId")
                if not task_id:
                    log_error(f"[CAPTCHA] CapSolver task creation failed: {data.get('errorDescription', data)}")
                    return None

            # Poll for result
            for _ in range(60):
                await asyncio.sleep(3)
                async with session.post("https://api.capsolver.com/getTaskResult",
                                        json={"clientKey": api_key, "taskId": task_id}) as r:
                    result = await r.json()
                    status = result.get("status")
                    if status == "ready":
                        token = result.get("solution", {}).get("gRecaptchaResponse")
                        if token:
                            log_ok("[CAPTCHA] Captcha solved successfully.")
                            return token
                        log_error("[CAPTCHA] No token in solution.")
                        return None
                    elif status == "failed":
                        log_error(f"[CAPTCHA] CapSolver failed: {result.get('errorDescription')}")
                        return None

            log_error("[CAPTCHA] Captcha solve timed out after 180s.")
            return None

    except Exception as e:
        log_error(f"[CAPTCHA] Solver error: {e}")
        return None

async def claim_vanity(vanity: str, guild_id: int, captcha_token: str = None) -> bool:
    """Attempt to claim vanity. Raises CloudflareBanError on long rate limits."""
    try:
        from discord.http import Route
        payload = {"code": vanity}
        if captcha_token:
            payload["captcha_key"] = captcha_token

        await bot.http.request(
            Route("PATCH", f"/guilds/{guild_id}/vanity-url"),
            json=payload,
        )
        return True
    except discord.HTTPException as e:
        # Strip HTML from error text for clean logs
        text = e.text or ""
        if text.strip().startswith("<"):
            text = "(HTML response — Cloudflare)"
        else:
            text = text[:200]

        if e.status == 429:
            retry = getattr(e, "retry_after", 1.0)
            if retry > 60:
                raise CloudflareBanError(retry)
            log_warn(f"[SNIPER] Rate limited — waiting {retry:.1f}s")
            await asyncio.sleep(retry)
        elif e.status == 401:
            log_error("[SNIPER] HTTP 401 — token invalid/expired. Get fresh token from Discord browser.")
            raise RuntimeError("invalid_token")
        elif e.status == 403:
            log_error(f"[SNIPER] HTTP 403 — no permission on guild {guild_id}. Are you the owner?")
            raise RuntimeError("no_permission")
        elif e.status == 400:
            # Check if it's a captcha challenge
            try:
                err_data = json.loads(text)
            except Exception:
                err_data = {}

            if "captcha_key" in err_data or "captcha_sitekey" in err_data:
                sitekey = err_data.get("captcha_sitekey", "a9b5fb07-92ff-493f-86fe-352a2803b3df")
                rqdata  = err_data.get("captcha_rqdata") or err_data.get("captcha_rqtoken")
                log_warn(f"[CAPTCHA] Discord requires captcha (sitekey: {sitekey[:16]}...)")
                token = await solve_captcha(sitekey, rqdata)
                if token:
                    return await claim_vanity(vanity, guild_id, captcha_token=token)
                # No solver available
                raise RuntimeError("captcha_unsolvable")
            else:
                log_warn(f"[SNIPER] HTTP 400: {text[:100]}")
        else:
            log_warn(f"[SNIPER] HTTP {e.status}: {text[:100]}")
        return False
    except (CloudflareBanError, RuntimeError):
        raise
    except Exception as e:
        log_warn(f"[SNIPER] Claim error: {type(e).__name__}: {e}")
        return False

async def sniper_loop_safe(vanity: str, guild_id: int):
    """Restarts sniper_loop on unexpected crashes indefinitely."""
    while True:
        try:
            await sniper_loop(vanity, guild_id)
            return  # clean exit (sniped or cancelled)
        except asyncio.CancelledError:
            log_warn("[SNIPER] Task cancelled.")
            return
        except RuntimeError as e:
            if str(e) == "captcha_unsolvable":
                log_error("[SNIPER] Captcha required but cannot solve — add CAPSOLVER_KEY to Railway env vars.")
                log_error("[SNIPER] Sniper paused for 60s then retrying (captcha may go away)...")
                await asyncio.sleep(60)
                # don't clear state — keep trying
            else:
                # Fatal errors (bad token, no permission) — stop entirely
                log_error(f"[SNIPER] Fatal error ({e}) — stopping. Fix the issue and restart with !snipe.")
                clear_snipe_state()
                return
        except Exception as e:
            log_error(f"[SNIPER] Unexpected crash: {e}")
            log_error(traceback.format_exc())
            log_warn("[SNIPER] Restarting in 15s...")
            await asyncio.sleep(15)

async def sniper_loop(vanity: str, guild_id: int):
    poll_interval      = 0.5   # poll every 500ms
    claim_interval     = 0.3   # 300ms between claims — stays under Cloudflare limit
    max_claim_attempts = 10    # 10 tries = 3s window per trigger
    poll_count         = 0
    log_every          = 120   # log "still watching" every 60 seconds (120 polls × 0.5s)

    log_ok(f"[SNIPER] Watching discord.gg/{vanity} | target guild: {guild_id}")
    log_info(f"[SNIPER] Poll: every {poll_interval}s | Claim: {max_claim_attempts}x every {claim_interval}s")

    while True:
        try:
            is_free = await check_vanity_free(vanity)
            poll_count += 1

            if poll_count % log_every == 0:
                log_info(f"[SNIPER] Still watching discord.gg/{vanity} — polls: {poll_count} ({poll_count * poll_interval / 60:.1f} min)")

            if is_free:
                log_warn(f"[SNIPER] discord.gg/{vanity} appears FREE — attempting to claim...")
                success = False

                for attempt in range(max_claim_attempts):
                    try:
                        result = await claim_vanity(vanity, guild_id)
                    except CloudflareBanError as ban:
                        log_error(f"[SNIPER] Cloudflare ban — pausing {ban.retry_after:.0f}s ({ban.retry_after/60:.1f} min)")
                        await asyncio.sleep(ban.retry_after)
                        break
                    except RuntimeError:
                        raise  # propagate fatal errors up

                    if result:
                        log_ok(f"[SNIPER] *** SNIPED discord.gg/{vanity} on attempt {attempt+1} ***")
                        clear_snipe_state()
                        return
                    await asyncio.sleep(claim_interval)

                if not success:
                    log_warn(f"[SNIPER] {max_claim_attempts} claim attempts failed — vanity likely on cooldown. Resuming poll...")

            await asyncio.sleep(poll_interval)

        except asyncio.CancelledError:
            log_warn("[SNIPER] Task cancelled.")
            return
        except RuntimeError:
            raise
        except Exception as e:
            log_error(f"[SNIPER] Loop error: {type(e).__name__}: {e}")
            await asyncio.sleep(5)

# ── EVENTS ────────────────────────────────────────────────────

@bot.event
async def on_ready():
    global _ready_done, snipe_task, _watchdog_task

    # on_ready fires on every reconnect — only do init once
    if _ready_done:
        log_info("Reconnected to gateway.")
        return
    _ready_done = True

    log_ok(f"Logged in as {bot.user} ({bot.user.id})")

    # Clean up any leftover temp files from previous session
    try:
        import glob
        stale = glob.glob(os.path.join(tempfile.gettempdir(), "tmp*"))
        removed = 0
        for f in stale:
            try:
                if os.path.isfile(f) and (datetime.datetime.now().timestamp() - os.path.getmtime(f)) > 3600:
                    os.unlink(f)
                    removed += 1
            except Exception:
                pass
        if removed:
            log_info(f"Cleaned up {removed} stale temp file(s)")
    except Exception as e:
        log_warn(f"Temp cleanup error: {e}")

    # Start periodic health logger (every 6 hours)
    async def health_logger():
        while True:
            await asyncio.sleep(6 * 3600)
            snipe = load_snipe_state()
            tc    = load_tc_state()
            tms   = load_tms_state()
            log_info(
                f"[HEALTH] uptime check — "
                f"sniper={'active' if snipe_task and not snipe_task.done() else 'idle'} "
                f"tc={'pending' if tc else 'idle'} "
                f"tms={'pending' if tms else 'idle'}"
            )
    asyncio.create_task(health_logger())

    log_info("Commands:")
    log_info("  !tm <channel_id> [batch]                   Upload media/ to a channel")
    log_info("  !tc <src_id> <dst_id> [batch]              Transfer files channel->channel (auto-resume)")
    log_info("  !tcreset                                   Clear TC resume state")
    log_info("  !tms <target_guild_id> <src1> <src2> ...   Multi-source transfer with channel creation (auto-resume)")
    log_info("  !tmsreset                                  Clear TMS resume state")
    log_info("  !kaboom [batch]                            Upload media/ to current channel")
    log_info("  !clone <src_cat_id> <dst_cat_id>           Clone one category")
    log_info("  !clones <dst_cat_id> <src1> <src2> ...     Clone multiple categories")
    log_info("  !bump                                      Start auto-bumper")

    # Auto-resume Sniper
    global snipe_task
    snipe = load_snipe_state()
    if snipe:
        log_warn(f"Resuming sniper: /{snipe['vanity']} -> guild {snipe['guild_id']}")
        snipe_task = asyncio.create_task(sniper_loop_safe(snipe["vanity"], snipe["guild_id"]))

    # Auto-resume TMS
    tms = load_tms_state()
    if tms:
        fail = tms.get("fail_count", 0)
        if fail >= 3:
            log_error(f"TMS failed {fail}x — clearing state. Restart manually with !tms")
            clear_tms_state()
        else:
            log_warn(f"Resuming TMS: guild={tms['target_guild_id']} ch_idx={tms['channel_index']} sent={tms['transferred']} fail={fail}")
            log_info("Starting in 5s... (!tmsreset to cancel)")
            await asyncio.sleep(5)
            tms = load_tms_state()
            if tms:
                g = bot.get_guild(tms["target_guild_id"])
                if not g:
                    log_error("TMS guild not found — clearing state")
                    clear_tms_state()
                else:
                    save_tms_state(tms["target_guild_id"], tms["src_ids"],
                                   tms["channel_index"], tms["last_message_id"],
                                   tms["transferred"], fail + 1)
                    await run_tms(g, tms["src_ids"],
                                  start_channel_index=tms["channel_index"],
                                  resume_after_msg_id=tms["last_message_id"],
                                  already_transferred=tms["transferred"])

    # Auto-resume TC
    tc = load_tc_state()
    if tc:
        fail = tc.get("fail_count", 0)
        if fail >= 3:
            log_error(f"TC failed {fail}x — clearing state. Restart manually with !tc")
            clear_tc_state()
        else:
            log_warn(f"Resuming TC: #{tc['source_id']} -> #{tc['target_id']} sent={tc['transferred']} fail={fail}")
            log_info("Starting in 5s... (!tcreset to cancel)")
            await asyncio.sleep(5)
            tc = load_tc_state()
            if tc:
                src = find_channel_by_id(tc["source_id"])
                dst = find_channel_by_id(tc["target_id"])
                if not src or not dst:
                    log_error("TC channel(s) not found — clearing state")
                    clear_tc_state()
                else:
                    save_tc_state(tc["source_id"], tc["target_id"], tc["batch_size"],
                                  tc["last_message_id"], tc["transferred"], fail + 1)
                    await run_tc(src, dst, tc["batch_size"],
                                 resume_after_id=tc["last_message_id"],
                                 already_transferred=tc["transferred"])

@bot.event
async def on_error(event, *args, **kwargs):
    log_error(f"Unhandled error in '{event}':")
    log_error(traceback.format_exc())

@bot.event
async def on_message(message):
    if message.author != bot.user: return
    content = message.content.strip()

    if content.startswith("!snipestop"):
        global snipe_task
        if snipe_task and not snipe_task.done():
            snipe_task.cancel()
            snipe_task = None
            clear_snipe_state()
            log_ok("Sniper stopped.")
        else:
            log_warn("No active sniper.")
        try: await message.delete()
        except Exception: pass

    elif content.startswith("!snipe"):
        parts = content.split()
        if len(parts) != 3:
            log_warn("Usage: !snipe <vanity> <guild_id>")
            return
        try: await message.delete()
        except Exception: pass
        vanity   = parts[1].lower().strip("/")
        try:
            guild_id = int(parts[2])
        except ValueError:
            log_warn("Guild ID must be a number.")
            return
        # Stop existing sniper if any
        if snipe_task and not snipe_task.done():
            snipe_task.cancel()
        save_snipe_state(vanity, guild_id)
        snipe_task = asyncio.create_task(sniper_loop_safe(vanity, guild_id))
        log_ok(f"Sniper armed: /{vanity} -> guild {guild_id}")

    elif content == "!tmsreset":
        clear_tms_state()
        log_ok("TMS state cleared.")
        try: await message.delete()
        except Exception: pass

    elif content == "!tcreset":
        clear_tc_state()
        log_ok("TC state cleared.")
        try: await message.delete()
        except Exception: pass

    elif content.startswith("!tms"):
        parts = content.split()
        if len(parts) < 3:
            log_warn("Usage: !tms <target_guild_id> <src_ch_id1> <src_ch_id2> ...")
            return
        try: await message.delete()
        except Exception: pass
        try:
            guild_id = int(parts[1])
            src_ids  = [int(x) for x in parts[2:]]
        except ValueError:
            log_warn("All IDs must be numbers.")
            return
        g = bot.get_guild(guild_id)
        if not g:
            log_error(f"Guild {guild_id} not found.")
            return
        save_tms_state(guild_id, src_ids, 0, None, 0, 0)
        await run_tms(g, src_ids)

    elif content.startswith("!tc"):
        parts = content.split()
        if len(parts) < 3:
            log_warn("Usage: !tc <source_id> <target_id> [batch_size]")
            return
        try: await message.delete()
        except Exception: pass
        try:
            src_id     = int(parts[1])
            dst_id     = int(parts[2])
            batch_size = max(1, min(int(parts[3]), 10)) if len(parts) >= 4 else 1
        except ValueError:
            log_warn("Usage: !tc <source_id> <target_id> [batch_size]")
            return
        src = find_channel_by_id(src_id)
        dst = find_channel_by_id(dst_id)
        if not src or not dst:
            log_error(f"Channel(s) not found — src:{src_id} dst:{dst_id}")
            return
        save_tc_state(src_id, dst_id, batch_size, None, 0, 0)
        await run_tc(src, dst, batch_size)

    elif content.startswith("!tm"):
        parts = content.split()
        if len(parts) < 2:
            log_warn("Usage: !tm <channel_id> [batch_size]")
            return
        try: await message.delete()
        except Exception: pass
        try:
            ch_id      = int(parts[1])
            batch_size = int(parts[2]) if len(parts) >= 3 else 1
        except ValueError:
            log_warn("Usage: !tm <channel_id> [batch_size]")
            return
        target = find_channel_by_id(ch_id)
        if not target:
            log_error(f"Channel {ch_id} not found.")
            return
        log_info(f"Uploading local media to #{target.name} (batch={batch_size})")
        await send_media_in_batches(target, batch_size)

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
            try: await clone_category(src_cat, dst_cat, guild)
            except Exception as e:
                log_error(f"[{idx}] Error: {e}")
                log_error(traceback.format_exc())
            await human_sleep(2.0, 5.0)
        log_ok(f"Multi-clone complete.")

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
            log_warn("IDs must be numbers.")
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
        try: await clone_category(src_cat, dst_cat, guild)
        except Exception as e:
            log_error(f"Clone error: {e}")
            log_error(traceback.format_exc())

    elif content.startswith("!kaboom"):
        parts = content.split()
        batch_size = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1
        try: await message.delete()
        except Exception: pass
        log_info(f"Kaboom — uploading to current channel (batch={batch_size})")
        try: await send_media_in_batches(message.channel, batch_size)
        except Exception as e:
            log_error(f"Kaboom error: {e}")
            log_error(traceback.format_exc())

    elif content.startswith("!bump"):
        try: await message.delete()
        except Exception: pass
        channel     = message.channel
        disboard_id = 302050872383242240
        log_info("Auto-bumper started.")
        while True:
            try:
                cmds = await channel.application_commands()
                cmd  = next((c for c in cmds if c.name == "bump" and c.application_id == disboard_id), None)
                if cmd: await cmd(); log_ok("Bump executed.")
                else: log_warn("/bump not found.")
            except discord.HTTPException as e:
                log_error(f"HTTP {e.status} during bump: {e.text}")
            except Exception as e:
                log_error(f"Bump error: {e}")
            wait = random.randint(6900, 7800)
            log_info(f"Next bump in {wait//60}m {wait%60}s")
            await asyncio.sleep(wait)


if __name__ == '__main__':
    initialize()
    TOKEN = getattr(config, 'TOKEN', None) or input("Discord profile token: ")
    bot.run(TOKEN)
