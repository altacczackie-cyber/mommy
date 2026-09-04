import datetime
import os
import shutil
import asyncio
import random
import traceback
import json
import tempfile
import aiohttp
import pyotp
import discord
from discord.ext import commands
import config

# ── CONSTANTS ─────────────────────────────────────────────────
TC_STATE_FILE   = "./tc_state.json"
TMS_STATE_FILE  = "./tms_state.json"
SNIPE_STATE_FILE = "./snipe_state.json"
SENT_IDS_FILE   = "./sent_ids.json"   # tracks sent attachment IDs to prevent duplicates

MAX_FILE_SIZE   = 500 * 1024 * 1024   # 500 MB
MAX_RETRIES     = 5
ADMIN_USER_ID   = 270644995390832651  # can DM commands to the bot

LARGE_MEDIA     = ""
CONTENT_FOLDER  = ""
LOG_FILE        = ""

bot = commands.Bot(command_prefix='!', self_bot=True)

_ready_done  = False
snipe_task   = None

# ── LOGGING ───────────────────────────────────────────────────

def ts():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def log(level, msg):
    print(f"[{ts()}] [{level}] {msg}", flush=True)

def log_info(msg):  log("INFO",  msg)
def log_warn(msg):  log("WARN",  msg)
def log_error(msg): log("ERROR", msg)
def log_ok(msg):    log("OK",    msg)

# ── WEBHOOK ───────────────────────────────────────────────────

async def webhook_log(msg: str, color: int = 0x5865F2):
    """Send a log entry to Discord webhook if WEBHOOK_URL is set."""
    url = os.environ.get("WEBHOOK_URL", "").strip()
    if not url:
        return
    try:
        payload = {
            "embeds": [{
                "description": f"`{ts()}`\n{msg}",
                "color": color,
            }]
        }
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await session.post(url, json=payload)
    except Exception:
        pass  # webhook is best-effort, never crash for it

def wh_info(msg):  asyncio.create_task(webhook_log(f"ℹ️ {msg}", 0x5865F2))
def wh_ok(msg):    asyncio.create_task(webhook_log(f"✅ {msg}", 0x57F287))
def wh_warn(msg):  asyncio.create_task(webhook_log(f"⚠️ {msg}", 0xFEE75C))
def wh_error(msg): asyncio.create_task(webhook_log(f"❌ {msg}", 0xED4245))

# ── ANTI-DETECTION ────────────────────────────────────────────

def night_multiplier():
    hour = datetime.datetime.now(datetime.timezone.utc).hour
    return random.uniform(2.5, 5.0) if 2 <= hour < 8 else 1.0

async def human_sleep(min_s=1.0, max_s=3.5):
    await asyncio.sleep(random.uniform(min_s, max_s) * night_multiplier())

# ── SENT IDS (duplicate prevention) ──────────────────────────

def load_sent_ids() -> set:
    if os.path.exists(SENT_IDS_FILE):
        try:
            with open(SENT_IDS_FILE) as f:
                return set(json.load(f))
        except Exception: pass
    return set()

def save_sent_id(att_id: int):
    ids = load_sent_ids()
    ids.add(str(att_id))
    with open(SENT_IDS_FILE, "w") as f:
        json.dump(list(ids), f)

def is_sent(att_id: int) -> bool:
    return str(att_id) in load_sent_ids()

# ── STATE ─────────────────────────────────────────────────────

def _write(path, data):
    with open(path, "w") as f: json.dump(data, f)

def _read(path):
    if os.path.exists(path):
        try:
            with open(path) as f: return json.load(f)
        except Exception: pass
    return None

def _del(path):
    if os.path.exists(path): os.remove(path)

def save_tc_state(source_id, target_id, batch_size, last_msg_id, transferred):
    _write(TC_STATE_FILE, {
        "source_id": source_id, "target_id": target_id,
        "batch_size": batch_size, "last_message_id": last_msg_id,
        "transferred": transferred,
    })

def load_tc_state():  return _read(TC_STATE_FILE)
def clear_tc_state(): _del(TC_STATE_FILE)

def save_tms_state(guild_id, src_ids, ch_idx, last_msg_id, transferred):
    _write(TMS_STATE_FILE, {
        "target_guild_id": guild_id, "src_ids": src_ids,
        "channel_index": ch_idx, "last_message_id": last_msg_id,
        "transferred": transferred,
    })

def load_tms_state():  return _read(TMS_STATE_FILE)
def clear_tms_state(): _del(TMS_STATE_FILE)

def save_snipe_state(vanity, guild_id):
    _write(SNIPE_STATE_FILE, {"vanity": vanity, "guild_id": guild_id})

def load_snipe_state():  return _read(SNIPE_STATE_FILE)
def clear_snipe_state(): _del(SNIPE_STATE_FILE)

# ── STREAMING DOWNLOAD ────────────────────────────────────────

async def stream_to_disk(url: str, filename: str, att_id: int = 0):
    """
    Download attachment to a temp file on disk (no RAM bloat).
    No retry limit — keeps trying until success or URL expiry.
    Returns (tmp_path, discord.File) or (None, None) if URL is dead.
    """
    suffix   = os.path.splitext(filename)[1] or ".bin"
    tmp_path = None
    attempt  = 0

    while True:
        try:
            tmp      = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            tmp_path = tmp.name
            tmp.close()

            # No total timeout — let it stream as long as needed
            timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=120)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        size_so_far = 0
                        with open(tmp_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(1024 * 512):
                                f.write(chunk)
                                size_so_far += len(chunk)
                        log_info(f"Downloaded {filename} ({size_so_far/1024/1024:.1f} MB)")
                        wh_info(f"Downloaded `{filename}` ({size_so_far/1024/1024:.1f} MB)")
                        return tmp_path, discord.File(tmp_path, filename=filename)

                    elif resp.status in (403, 404):
                        # URL expired — can't retry
                        log_warn(f"Attachment URL expired ({resp.status}): {filename} — skipping")
                        wh_warn(f"Skipped `{filename}` — URL expired ({resp.status})")
                        if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
                        return None, None

                    else:
                        log_warn(f"HTTP {resp.status} downloading {filename} — retrying...")
                        if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
                        await asyncio.sleep(min(2 ** attempt, 60))
                        attempt += 1
                        continue

        except asyncio.TimeoutError:
            log_warn(f"Read timeout on {filename} (attempt {attempt+1}) — retrying...")
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            await asyncio.sleep(min(10 * (attempt + 1), 120))
            attempt += 1

        except (asyncio.CancelledError, GeneratorExit):
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            return None, None

        except Exception as e:
            log_warn(f"Download error {filename} (attempt {attempt+1}): {e}")
            if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)
            await asyncio.sleep(min(2 ** attempt, 60))
            attempt += 1

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
            wait = 5.0
            try: wait = float(e.response.headers.get("Retry-After", 5)) + random.uniform(0.5, 2.0)
            except Exception: pass
            log_warn(f"Rate limited — waiting {wait:.1f}s")
            await asyncio.sleep(wait)
            if attempt < MAX_RETRIES:
                return await safe_send(channel, content=content, files=files, attempt=attempt+1)
            return None
        elif e.status in (500, 502, 503, 504):
            wait = min(2 ** attempt + random.uniform(0, 1), 60)
            log_warn(f"Discord {e.status} — retrying in {wait:.1f}s")
            await asyncio.sleep(wait)
            if attempt < MAX_RETRIES:
                return await safe_send(channel, content=content, files=files, attempt=attempt+1)
            return None
        else:
            log_error(f"Send HTTP {e.status}: {e.text[:100]}")
            return None
    except (asyncio.TimeoutError, discord.ConnectionClosed) as e:
        wait = min(2 ** attempt + random.uniform(0, 1), 60)
        log_warn(f"{type(e).__name__} sending — retrying in {wait:.1f}s")
        await asyncio.sleep(wait)
        if attempt < MAX_RETRIES:
            return await safe_send(channel, content=content, files=files, attempt=attempt+1)
        return None
    except (asyncio.CancelledError, GeneratorExit):
        return None
    except Exception as e:
        log_error(f"Unexpected send error: {e}")
        return None

# ── INIT ──────────────────────────────────────────────────────

def initialize():
    global CONTENT_FOLDER, LOG_FILE, LARGE_MEDIA
    cwd            = os.getcwd()
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
        already = f.read()

    all_files = []
    for folder in [CONTENT_FOLDER, LARGE_MEDIA]:
        for fname in os.listdir(folder):
            fpath = os.path.join(folder, fname)
            if not os.path.isfile(fpath) or fname in already: continue
            size_b  = os.path.getsize(fpath)
            size_mb = size_b / (1024 * 1024)
            if size_b > MAX_FILE_SIZE:
                log_warn(f"Skipped {fname} ({size_mb:.1f} MB) — over 500 MB")
                if folder == CONTENT_FOLDER:
                    shutil.move(fpath, os.path.join(LARGE_MEDIA, fname))
                continue
            all_files.append((fpath, fname, size_mb))

    random.shuffle(all_files)
    total, uploaded, failed = len(all_files), 0, 0
    if total == 0:
        log_info("No new files to upload.")
        return
    log_info(f"Uploading {total} file(s) in batches of {batch_size}")
    wh_info(f"Starting upload of **{total}** file(s) to `#{channel.name}`")

    for i in range(0, total, batch_size):
        batch = all_files[i:i + batch_size]
        names = ", ".join(f[1] for f in batch)
        log_info(f"Batch [{i+1}-{min(i+batch_size, total)}/{total}]: {names}")
        dfiles = []
        for fpath, fname, _ in batch:
            try: dfiles.append(discord.File(fpath, fname))
            except Exception as e:
                log_error(f"Cannot open {fname}: {e}")
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

    log_ok(f"Upload done — sent: {uploaded} | failed: {failed} | total: {total}")
    wh_ok(f"Upload to `#{channel.name}` complete — sent: **{uploaded}** | failed: **{failed}**")

# ── TC: CHANNEL TRANSFER ──────────────────────────────────────

async def run_tc(src, dst, batch_size, resume_after_id=None, already_transferred=0):
    source_id, target_id = src.id, dst.id
    file_batch, tmp_paths = [], []
    total, skipped = already_transferred, 0
    last_msg_id = resume_after_id

    msg = f"Transfer `#{src.name}` → `#{dst.name}` (batch={batch_size})" + (f" | resume after `{resume_after_id}`" if resume_after_id else "")
    log_info(msg)
    wh_info(msg)

    try:
        kwargs = {"limit": None, "oldest_first": True}
        if resume_after_id:
            kwargs["after"] = discord.Object(id=resume_after_id)

        async for msg_obj in src.history(**kwargs):
            msg_files, msg_tmps = [], []

            for att in msg_obj.attachments:
                # Skip already-sent attachments (duplicate prevention)
                if is_sent(att.id):
                    log_info(f"Skipping already-sent: {att.filename}")
                    continue
                if att.size > MAX_FILE_SIZE:
                    log_warn(f"Skipped oversized {att.filename} ({att.size/1024/1024:.1f} MB)")
                    skipped += 1
                    continue
                log_info(f"Downloading {att.filename} ({att.size/1024/1024:.1f} MB)...")
                wh_info(f"Downloading `{att.filename}` ({att.size/1024/1024:.1f} MB)")
                tmp_path, dfile = await stream_to_disk(att.url, att.filename, att.id)
                if dfile:
                    msg_files.append((att.id, dfile))
                    msg_tmps.append(tmp_path)
                else:
                    skipped += 1

            file_batch.extend(msg_files)
            tmp_paths.extend(msg_tmps)

            while len(file_batch) >= batch_size:
                chunk      = file_batch[:batch_size]
                chunk_tmps = tmp_paths[:batch_size]
                file_batch = file_batch[batch_size:]
                tmp_paths  = tmp_paths[batch_size:]
                att_ids    = [c[0] for c in chunk]
                dfiles     = [c[1] for c in chunk]
                result     = await safe_send(dst, files=dfiles)
                cleanup_tmp(*chunk_tmps)
                if result:
                    total += len(dfiles)
                    for aid in att_ids:
                        save_sent_id(aid)
                    log_ok(f"Sent {len(dfiles)} file(s) (total: {total})")
                    wh_ok(f"Sent **{len(dfiles)}** file(s) — total: **{total}**")
                await human_sleep(1.5, 4.0)

            # Checkpoint per message — prevent duplicates on resume
            last_msg_id = msg_obj.id
            save_tc_state(source_id, target_id, batch_size, last_msg_id, total)

        # Flush remaining
        if file_batch:
            att_ids = [c[0] for c in file_batch]
            dfiles  = [c[1] for c in file_batch]
            result  = await safe_send(dst, files=dfiles)
            cleanup_tmp(*tmp_paths)
            if result:
                total += len(dfiles)
                for aid in att_ids:
                    save_sent_id(aid)

    except discord.Forbidden:
        log_error("No permission to read source channel — clearing state")
        wh_error("TC stopped — no permission to read source channel")
        cleanup_tmp(*tmp_paths)
        clear_tc_state()
        return
    except (asyncio.CancelledError, GeneratorExit, Exception) as e:
        if not isinstance(e, Exception):
            log_warn(f"{type(e).__name__} — saving checkpoint")
        else:
            log_error(f"TC error: {e}")
            log_error(traceback.format_exc())
            wh_error(f"TC error: `{type(e).__name__}: {str(e)[:100]}`")
        cleanup_tmp(*tmp_paths)
        save_tc_state(source_id, target_id, batch_size, last_msg_id, total)
        log_warn("Checkpoint saved — will resume on restart")
        return

    clear_tc_state()
    log_ok(f"TC complete — total: {total} | skipped: {skipped}")
    wh_ok(f"TC `#{src.name}` → `#{dst.name}` complete — **{total}** sent | **{skipped}** skipped")

# ── TMS: MULTI SOURCE ─────────────────────────────────────────

async def run_tms(target_guild, src_ids, start_ch_idx=0, resume_after_msg_id=None, already_transferred=0):
    src_ids_sorted = sorted(src_ids)
    total_chs      = len(src_ids_sorted)
    total          = already_transferred

    log_info(f"TMS: {total_chs} channel(s) → guild '{target_guild.name}'")
    wh_info(f"TMS started — **{total_chs}** channel(s) → `{target_guild.name}`")

    for ch_idx in range(start_ch_idx, total_chs):
        src_id = src_ids_sorted[ch_idx]
        src    = find_channel_by_id(src_id)

        if not src:
            log_error(f"[{ch_idx+1}/{total_chs}] Source {src_id} not found — skipping")
            save_tms_state(target_guild.id, src_ids, ch_idx + 1, None, total)
            continue

        log_info(f"[{ch_idx+1}/{total_chs}] #{src.name} ({src_id})")
        wh_info(f"TMS [{ch_idx+1}/{total_chs}] starting `#{src.name}`")

        # Find or create destination channel
        dst = discord.utils.get(target_guild.text_channels, name=src.name)
        if dst:
            log_warn(f"#{src.name} already exists in target — using it")
        else:
            dst = None
            for attempt in range(999):  # infinite retries, no abort
                try:
                    dst = await target_guild.create_text_channel(src.name, nsfw=True)
                    log_ok(f"Created #{dst.name}")
                    break
                except discord.HTTPException as e:
                    if e.status == 429:
                        wait = 5 + random.uniform(1, 3)
                        log_warn(f"Rate limited creating channel — {wait:.1f}s")
                        await asyncio.sleep(wait)
                    elif e.status == 403:
                        log_error("No permission to create channels — aborting TMS")
                        wh_error("TMS aborted — no permission to create channels")
                        clear_tms_state()
                        return
                    else:
                        log_error(f"HTTP {e.status} creating #{src.name}: {e.text}")
                        await asyncio.sleep(min(2 ** attempt, 60))
                except (asyncio.CancelledError, GeneratorExit):
                    save_tms_state(target_guild.id, src_ids, ch_idx, None, total)
                    return
                except Exception as e:
                    log_error(f"Error creating channel: {e}")
                    await asyncio.sleep(10)
            if not dst:
                save_tms_state(target_guild.id, src_ids, ch_idx + 1, None, total)
                continue

        resume_id   = resume_after_msg_id if ch_idx == start_ch_idx else None
        file_batch  = []
        tmp_paths   = []
        last_msg_id = resume_id
        ch_sent     = 0
        ch_skipped  = 0

        try:
            kwargs = {"limit": None, "oldest_first": True}
            if resume_id:
                kwargs["after"] = discord.Object(id=resume_id)

            async for msg_obj in src.history(**kwargs):
                msg_files, msg_tmps = [], []

                for att in msg_obj.attachments:
                    if is_sent(att.id):
                        log_info(f"  Skipping already-sent: {att.filename}")
                        continue
                    if att.size > MAX_FILE_SIZE:
                        log_warn(f"  Skipped oversized {att.filename}")
                        ch_skipped += 1
                        continue
                    log_info(f"  [{ch_idx+1}/{total_chs}] Downloading {att.filename} ({att.size/1024/1024:.1f} MB)...")
                    wh_info(f"[{ch_idx+1}/{total_chs}] `#{src.name}` → downloading `{att.filename}` ({att.size/1024/1024:.1f} MB)")
                    tmp_path, dfile = await stream_to_disk(att.url, att.filename, att.id)
                    if dfile:
                        msg_files.append((att.id, dfile))
                        msg_tmps.append(tmp_path)
                    else:
                        ch_skipped += 1

                file_batch.extend(msg_files)
                tmp_paths.extend(msg_tmps)

                while len(file_batch) >= 5:
                    chunk      = file_batch[:5]
                    chunk_tmps = tmp_paths[:5]
                    file_batch = file_batch[5:]
                    tmp_paths  = tmp_paths[5:]
                    att_ids    = [c[0] for c in chunk]
                    dfiles     = [c[1] for c in chunk]
                    result     = await safe_send(dst, files=dfiles)
                    cleanup_tmp(*chunk_tmps)
                    if result:
                        ch_sent += len(dfiles)
                        total   += len(dfiles)
                        for aid in att_ids:
                            save_sent_id(aid)
                        log_ok(f"  [{ch_idx+1}/{total_chs}] Sent {len(dfiles)} file(s) — ch total: {ch_sent}")
                        wh_ok(f"[{ch_idx+1}/{total_chs}] `#{src.name}` sent **{len(dfiles)}** file(s) — total: **{total}**")
                    await human_sleep(1.5, 4.0)

                last_msg_id = msg_obj.id
                save_tms_state(target_guild.id, src_ids, ch_idx, last_msg_id, total)

            # Flush remaining
            if file_batch:
                att_ids = [c[0] for c in file_batch]
                dfiles  = [c[1] for c in file_batch]
                result  = await safe_send(dst, files=dfiles)
                cleanup_tmp(*tmp_paths)
                if result:
                    ch_sent += len(dfiles)
                    total   += len(dfiles)
                    for aid in att_ids:
                        save_sent_id(aid)
                file_batch, tmp_paths = [], []

        except discord.Forbidden:
            log_error(f"No read permission for #{src.name} — skipping channel")
            wh_warn(f"No read permission for `#{src.name}` — skipping")
            cleanup_tmp(*tmp_paths)
            save_tms_state(target_guild.id, src_ids, ch_idx + 1, None, total)
            resume_after_msg_id = None
            await human_sleep(2.0, 5.0)
            continue
        except (asyncio.CancelledError, GeneratorExit, Exception) as e:
            if not isinstance(e, Exception):
                log_warn(f"{type(e).__name__} in TMS — checkpoint saved")
            else:
                log_error(f"TMS error on #{src.name}: {e}")
                log_error(traceback.format_exc())
                wh_error(f"TMS error on `#{src.name}`: `{type(e).__name__}: {str(e)[:100]}`")
            cleanup_tmp(*tmp_paths)
            save_tms_state(target_guild.id, src_ids, ch_idx, last_msg_id, total)
            return

        log_ok(f"[{ch_idx+1}/{total_chs}] #{src.name} done — sent: {ch_sent} | skipped: {ch_skipped}")
        wh_ok(f"[{ch_idx+1}/{total_chs}] `#{src.name}` done — **{ch_sent}** sent | **{ch_skipped}** skipped")
        resume_after_msg_id = None
        save_tms_state(target_guild.id, src_ids, ch_idx + 1, None, total)
        await human_sleep(2.0, 5.0)

    clear_tms_state()
    log_ok(f"TMS complete — total: {total}")
    wh_ok(f"TMS complete — **{total}** files transferred to `{target_guild.name}`")

# ── CLONE ─────────────────────────────────────────────────────

async def clone_category(source_cat, target_cat, guild):
    log_info(f"Cloning '{source_cat.name}' → '{target_cat.name}'")
    wh_info(f"Clone `{source_cat.name}` → `{target_cat.name}`")
    existing = {c.name.lower() for c in target_cat.text_channels}
    channels = list(source_cat.text_channels)
    cloned, skipped, msgs_ok, msgs_err = 0, 0, 0, 0

    for ch in channels:
        if ch.name.lower() in existing:
            log_warn(f"Skipping #{ch.name} — already exists")
            skipped += 1
            continue

        new_ch = None
        for attempt in range(999):
            try:
                new_ch = await guild.create_text_channel(ch.name, category=target_cat, nsfw=True)
                log_info(f"Created #{new_ch.name}")
                cloned += 1
                break
            except discord.HTTPException as e:
                if e.status == 429:
                    wait = 5 + random.uniform(0.5, 2)
                    log_warn(f"Rate limited — {wait:.1f}s")
                    await asyncio.sleep(wait)
                elif e.status == 403:
                    log_error(f"No permission — skipping #{ch.name}")
                    skipped += 1
                    break
                else:
                    log_error(f"HTTP {e.status} creating #{ch.name}: {e.text}")
                    await asyncio.sleep(min(2 ** attempt, 60))
            except (asyncio.CancelledError, GeneratorExit):
                return
            except Exception as e:
                log_error(f"Error creating #{ch.name}: {e}")
                skipped += 1
                break

        if not new_ch: continue

        msg_count = 0
        try:
            async for m in ch.history(limit=None, oldest_first=True):
                content = m.content or ""
                files, tmps = [], []
                for att in m.attachments:
                    if att.size > MAX_FILE_SIZE:
                        log_warn(f"Skipped oversized {att.filename}")
                        continue
                    tmp_path, dfile = await stream_to_disk(att.url, att.filename, att.id)
                    if dfile:
                        files.append(dfile)
                        tmps.append(tmp_path)

                if files:
                    for j in range(0, len(files), 10):
                        chunk   = files[j:j+10]
                        caption = f"**{m.author.name}**: {content}" if content and j == 0 else None
                        result  = await safe_send(new_ch, content=caption, files=chunk)
                        cleanup_tmp(*tmps[j:j+10])
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
                    log_info(f"#{ch.name}: {msg_count} messages...")
        except discord.Forbidden:
            log_error(f"No read permission for #{ch.name}")
        except (asyncio.CancelledError, GeneratorExit, Exception) as e:
            if isinstance(e, Exception):
                log_error(f"Error in #{ch.name}: {e}")
        log_ok(f"#{ch.name} — {msg_count} msgs | sent: {msgs_ok} | err: {msgs_err}")
        await human_sleep(1.5, 4.0)

    log_ok(f"Clone done — {cloned} channels | msgs ok: {msgs_ok}")
    wh_ok(f"Clone `{source_cat.name}` done — **{cloned}** channels | **{msgs_ok}** messages sent")

# ── VANITY SNIPER ─────────────────────────────────────────────

def get_totp_code():
    secret = os.environ.get("TOTP_SECRET", "").strip().upper().replace(" ", "")
    if not secret: return None
    try: return pyotp.TOTP(secret).now()
    except Exception as e:
        log_error(f"[TOTP] Failed: {e}")
        return None

async def check_vanity_free(vanity: str) -> bool:
    try:
        from discord.http import Route
        data = await bot.http.request(Route("GET", f"/invites/{vanity}"))
        return "guild" not in data
    except discord.NotFound:
        return True
    except discord.HTTPException as e:
        if e.status == 429:
            retry = getattr(e, "retry_after", 1.0)
            log_warn(f"[SNIPER] Rate limited checking — {retry:.1f}s")
            await asyncio.sleep(retry)
        return False
    except Exception as e:
        log_warn(f"[SNIPER] Check error: {e}")
        return False

class CloudflareBanError(Exception):
    def __init__(self, retry_after): self.retry_after = retry_after

async def claim_vanity(vanity: str, guild_id: int, captcha_token: str = None) -> bool:
    try:
        from discord.http import Route
        payload = {"code": vanity}
        if captcha_token: payload["captcha_key"] = captcha_token
        totp = get_totp_code()
        if totp:
            payload["mfa_totp_code"]   = totp
            payload["mfa_totp_ticket"] = ""
        await bot.http.request(Route("PATCH", f"/guilds/{guild_id}/vanity-url"), json=payload)
        return True
    except discord.HTTPException as e:
        text = (e.text or "")
        if text.strip().startswith("<"): text = "(HTML — Cloudflare)"
        else: text = text[:150]

        if e.status == 429:
            retry = getattr(e, "retry_after", 1.0)
            if retry > 60: raise CloudflareBanError(retry)
            log_warn(f"[SNIPER] Rate limited — {retry:.1f}s")
            await asyncio.sleep(retry)
        elif e.status == 401:
            err_l = text.lower()
            if "two_factor" in err_l or "two factor" in err_l or "mfa" in err_l:
                totp = get_totp_code()
                if totp and not captcha_token:
                    log_warn(f"[SNIPER] 2FA required — retrying with TOTP {totp}")
                    return await claim_vanity(vanity, guild_id, captcha_token=captcha_token)
                log_error("[SNIPER] 2FA required but no TOTP code — check TOTP_SECRET")
                raise RuntimeError("2fa_required")
            else:
                log_error(f"[SNIPER] HTTP 401: {text}")
                raise RuntimeError("invalid_token")
        elif e.status == 403:
            log_error(f"[SNIPER] HTTP 403 — not owner of guild {guild_id}")
            raise RuntimeError("no_permission")
        elif e.status == 400:
            try: err_data = json.loads(text)
            except Exception: err_data = {}
            if "captcha_key" in err_data or "captcha_sitekey" in err_data:
                log_warn("[SNIPER] CAPTCHA required — need CAPSOLVER_KEY env var")
                raise RuntimeError("captcha_required")
            log_warn(f"[SNIPER] HTTP 400: {text[:100]}")
        else:
            log_warn(f"[SNIPER] HTTP {e.status}: {text[:100]}")
        return False
    except (CloudflareBanError, RuntimeError): raise
    except Exception as e:
        log_warn(f"[SNIPER] Claim error: {e}")
        return False

async def sniper_loop_safe(vanity: str, guild_id: int):
    """Outer wrapper — always restarts on crash, never gives up."""
    while True:
        try:
            await sniper_loop(vanity, guild_id)
            return  # clean exit
        except asyncio.CancelledError:
            log_warn("[SNIPER] Task cancelled.")
            return
        except RuntimeError as e:
            err = str(e)
            if err == "2fa_required":
                log_error("[SNIPER] 2FA required — fix TOTP_SECRET then restart with !snipe")
                wh_error("Sniper paused — 2FA required. Fix TOTP_SECRET and restart with `!snipe`")
                await asyncio.sleep(300)
            elif err == "captcha_required":
                log_error("[SNIPER] CAPTCHA required — add CAPSOLVER_KEY to Railway")
                wh_error("Sniper paused — CAPTCHA required. Add CAPSOLVER_KEY to Railway env vars")
                await asyncio.sleep(60)
            elif err in ("invalid_token", "no_permission"):
                log_error(f"[SNIPER] Fatal ({err}) — stopping. Fix and restart with !snipe")
                wh_error(f"Sniper stopped — fatal error: `{err}`")
                clear_snipe_state()
                return
            else:
                await asyncio.sleep(15)
        except Exception as e:
            log_error(f"[SNIPER] Crashed: {e}")
            log_error(traceback.format_exc())
            wh_error(f"Sniper crashed: `{type(e).__name__}: {str(e)[:100]}` — restarting in 15s")
            await asyncio.sleep(15)

async def sniper_loop(vanity: str, guild_id: int):
    poll_interval      = 0.5
    claim_interval     = 0.3
    max_claim_attempts = 10
    poll_count         = 0
    log_every          = 120   # every 60 seconds

    log_ok(f"[SNIPER] Watching discord.gg/{vanity} → guild {guild_id}")
    wh_info(f"Sniper armed — watching `discord.gg/{vanity}` → guild `{guild_id}`")

    while True:
        try:
            is_free   = await check_vanity_free(vanity)
            poll_count += 1

            if poll_count % log_every == 0:
                log_info(f"[SNIPER] Still watching discord.gg/{vanity} — {poll_count} polls ({poll_count*poll_interval/60:.1f} min)")

            if is_free:
                log_warn(f"[SNIPER] discord.gg/{vanity} FREE — claiming...")
                wh_warn(f"Sniper: `discord.gg/{vanity}` appears **FREE** — attempting claim!")
                success = False
                for attempt in range(max_claim_attempts):
                    try:
                        result = await claim_vanity(vanity, guild_id)
                    except CloudflareBanError as ban:
                        log_error(f"[SNIPER] Cloudflare ban — pausing {ban.retry_after:.0f}s ({ban.retry_after/60:.1f} min)")
                        wh_error(f"Sniper: Cloudflare ban — pausing {ban.retry_after/60:.1f} min")
                        await asyncio.sleep(ban.retry_after)
                        break
                    except RuntimeError: raise
                    if result:
                        log_ok(f"[SNIPER] *** SNIPED discord.gg/{vanity} on attempt {attempt+1} ***")
                        wh_ok(f"🎯 **SNIPED** `discord.gg/{vanity}` on attempt **{attempt+1}**!")
                        clear_snipe_state()
                        return
                    await asyncio.sleep(claim_interval)

                if not success:
                    log_warn(f"[SNIPER] {max_claim_attempts} attempts failed — cooldown. Resuming poll...")

            await asyncio.sleep(poll_interval)

        except asyncio.CancelledError: raise
        except RuntimeError: raise
        except Exception as e:
            log_error(f"[SNIPER] Loop error: {e}")
            await asyncio.sleep(5)

# ── HELP TEXT ─────────────────────────────────────────────────

HELP_TEXT = """**Bot Commands:**
`!tm <channel_id> [batch]` — Upload local media/ to a channel
`!tc <src_id> <dst_id> [batch]` — Transfer files channel→channel (auto-resume, no duplicates)
`!tcreset` — Clear TC resume state
`!tms <guild_id> <src1> <src2> ...` — Multi-source transfer with channel creation (auto-resume)
`!tmsreset` — Clear TMS resume state
`!kaboom [batch]` — Upload local media/ to current channel
`!clone <src_cat_id> <dst_cat_id>` — Clone one category
`!clones <dst_cat_id> <src1> <src2> ...` — Clone multiple categories
`!bump` — Start auto-bumper
`!snipe <vanity> <guild_id>` — Start vanity sniper (24/7, auto-resume)
`!snipestop` — Stop sniper
`!status` — Show current bot status
`!help` — Show this message"""

# ── EVENTS ────────────────────────────────────────────────────

@bot.event
async def on_ready():
    global _ready_done, snipe_task
    if _ready_done:
        log_info("Gateway reconnected.")
        return
    _ready_done = True

    log_ok(f"Logged in as {bot.user} ({bot.user.id})")
    wh_ok(f"Bot online as `{bot.user}` (`{bot.user.id}`)")

    # Cleanup stale temp files
    try:
        import glob
        removed = 0
        for f in glob.glob(os.path.join(tempfile.gettempdir(), "tmp*")):
            try:
                if os.path.isfile(f) and (datetime.datetime.now().timestamp() - os.path.getmtime(f)) > 3600:
                    os.unlink(f)
                    removed += 1
            except Exception: pass
        if removed:
            log_info(f"Cleaned {removed} stale temp file(s)")
    except Exception: pass

    # Health logger every 6 hours
    async def health_logger():
        while True:
            await asyncio.sleep(6 * 3600)
            snipe = load_snipe_state()
            tc    = load_tc_state()
            tms   = load_tms_state()
            msg   = (f"Health check — sniper: {'active /' + snipe['vanity'] if snipe else 'idle'} | "
                     f"tc: {'pending' if tc else 'idle'} | tms: {'pending' if tms else 'idle'}")
            log_info(f"[HEALTH] {msg}")
            wh_info(f"Health: {msg}")
    asyncio.create_task(health_logger())

    # Print commands
    for line in HELP_TEXT.split("\n"):
        log_info(line)

    # Auto-resume sniper
    snipe = load_snipe_state()
    if snipe:
        log_warn(f"Auto-resuming sniper: /{snipe['vanity']} → {snipe['guild_id']}")
        wh_warn(f"Resuming sniper: `discord.gg/{snipe['vanity']}`")
        snipe_task = asyncio.create_task(sniper_loop_safe(snipe["vanity"], snipe["guild_id"]))

    # Auto-resume TMS
    tms = load_tms_state()
    if tms:
        log_warn(f"Auto-resuming TMS: guild={tms['target_guild_id']} ch={tms['channel_index']} sent={tms['transferred']}")
        wh_warn(f"Resuming TMS from channel index **{tms['channel_index']}** — already sent: **{tms['transferred']}**")
        await asyncio.sleep(3)
        g = bot.get_guild(tms["target_guild_id"])
        if not g:
            log_error("TMS guild not found — clearing state")
            clear_tms_state()
        else:
            asyncio.create_task(run_tms(g, tms["src_ids"],
                                        start_ch_idx=tms["channel_index"],
                                        resume_after_msg_id=tms["last_message_id"],
                                        already_transferred=tms["transferred"]))

    # Auto-resume TC
    tc = load_tc_state()
    if tc:
        log_warn(f"Auto-resuming TC: #{tc['source_id']} → #{tc['target_id']} sent={tc['transferred']}")
        wh_warn(f"Resuming TC — already sent: **{tc['transferred']}**")
        await asyncio.sleep(3)
        src = find_channel_by_id(tc["source_id"])
        dst = find_channel_by_id(tc["target_id"])
        if not src or not dst:
            log_error("TC channel(s) not found — clearing state")
            clear_tc_state()
        else:
            asyncio.create_task(run_tc(src, dst, tc["batch_size"],
                                       resume_after_id=tc["last_message_id"],
                                       already_transferred=tc["transferred"]))

@bot.event
async def on_error(event, *args, **kwargs):
    log_error(f"Unhandled error in '{event}':")
    log_error(traceback.format_exc())

async def handle_command(message):
    """Shared command handler for both selfbot and admin DMs."""
    content = message.content.strip()
    channel = message.channel
    guild   = message.guild

    if content == "!help":
        await message.channel.send(HELP_TEXT)

    elif content == "!status":
        snipe = load_snipe_state()
        tc    = load_tc_state()
        tms   = load_tms_state()
        lines = [
            f"**Sniper:** {'active — `discord.gg/' + snipe['vanity'] + '`' if snipe else 'idle'}",
            f"**TC:** {'pending — `#' + str(tc.get('source_id','?')) + '` → `#' + str(tc.get('target_id','?')) + '`' if tc else 'idle'}",
            f"**TMS:** {'pending — ch idx ' + str(tms.get('channel_index','?')) + ', sent ' + str(tms.get('transferred','?')) if tms else 'idle'}",
        ]
        await message.channel.send("\n".join(lines))

    elif content == "!snipestop":
        global snipe_task
        if snipe_task and not snipe_task.done():
            snipe_task.cancel()
            snipe_task = None
            clear_snipe_state()
            log_ok("Sniper stopped.")
            wh_warn("Sniper stopped by user command")
        else:
            log_warn("No active sniper.")
        try: await message.delete()
        except Exception: pass

    elif content.startswith("!snipe "):
        parts = content.split()
        if len(parts) != 3:
            await message.channel.send("Usage: `!snipe <vanity> <guild_id>`")
            return
        vanity = parts[1].lower().strip("/")
        try: gid = int(parts[2])
        except ValueError:
            await message.channel.send("Guild ID must be a number.")
            return
        if snipe_task and not snipe_task.done():
            snipe_task.cancel()
        save_snipe_state(vanity, gid)
        snipe_task = asyncio.create_task(sniper_loop_safe(vanity, gid))
        log_ok(f"Sniper armed: /{vanity} → {gid}")
        wh_ok(f"Sniper armed: `discord.gg/{vanity}` → guild `{gid}`")
        try: await message.delete()
        except Exception: pass

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

    elif content.startswith("!tms "):
        parts = content.split()
        if len(parts) < 3:
            await message.channel.send("Usage: `!tms <guild_id> <src_ch_id1> <src_ch_id2> ...`")
            return
        try: await message.delete()
        except Exception: pass
        try:
            gid     = int(parts[1])
            src_ids = [int(x) for x in parts[2:]]
        except ValueError:
            await message.channel.send("All IDs must be numbers.")
            return
        g = bot.get_guild(gid)
        if not g:
            log_error(f"Guild {gid} not found.")
            return
        save_tms_state(gid, src_ids, 0, None, 0)
        asyncio.create_task(run_tms(g, src_ids))

    elif content.startswith("!tc "):
        parts = content.split()
        if len(parts) < 3:
            await message.channel.send("Usage: `!tc <src_id> <dst_id> [batch]`")
            return
        try: await message.delete()
        except Exception: pass
        try:
            src_id = int(parts[1])
            dst_id = int(parts[2])
            batch  = max(1, min(int(parts[3]), 10)) if len(parts) >= 4 else 1
        except ValueError:
            return
        src = find_channel_by_id(src_id)
        dst = find_channel_by_id(dst_id)
        if not src or not dst:
            log_error(f"Channel(s) not found — src:{src_id} dst:{dst_id}")
            return
        save_tc_state(src_id, dst_id, batch, None, 0)
        asyncio.create_task(run_tc(src, dst, batch))

    elif content.startswith("!tm "):
        parts = content.split()
        if len(parts) < 2: return
        try: await message.delete()
        except Exception: pass
        try:
            ch_id = int(parts[1])
            batch = int(parts[2]) if len(parts) >= 3 else 1
        except ValueError: return
        target = find_channel_by_id(ch_id)
        if not target:
            log_error(f"Channel {ch_id} not found.")
            return
        asyncio.create_task(send_media_in_batches(target, batch))

    elif content.startswith("!clones "):
        parts = content.split()
        if len(parts) < 3: return
        try: await message.delete()
        except Exception: pass
        try:
            dst_id  = int(parts[1])
            src_ids = [int(x) for x in parts[2:]]
        except ValueError: return
        dst_cat = guild.get_channel(dst_id) if guild else None
        if not dst_cat or not isinstance(dst_cat, discord.CategoryChannel):
            log_error(f"Category {dst_id} not found.")
            return
        for idx, sid in enumerate(src_ids, 1):
            src_cat = find_category(sid)
            if not src_cat:
                log_error(f"[{idx}/{len(src_ids)}] Source {sid} not found — skipping")
                continue
            asyncio.create_task(clone_category(src_cat, dst_cat, guild))
            await human_sleep(2.0, 5.0)

    elif content.startswith("!clone "):
        parts = content.split()
        if len(parts) != 3: return
        try: await message.delete()
        except Exception: pass
        try:
            src_id = int(parts[1])
            dst_id = int(parts[2])
        except ValueError: return
        src_cat = find_category(src_id)
        dst_cat = guild.get_channel(dst_id) if guild else None
        if not src_cat or not isinstance(dst_cat, discord.CategoryChannel): return
        asyncio.create_task(clone_category(src_cat, dst_cat, guild))

    elif content.startswith("!kaboom"):
        parts = content.split()
        batch = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1
        try: await message.delete()
        except Exception: pass
        asyncio.create_task(send_media_in_batches(channel, batch))

    elif content.startswith("!bump"):
        try: await message.delete()
        except Exception: pass
        disboard_id = 302050872383242240
        log_info("Auto-bumper started.")
        while True:
            try:
                cmds = await channel.application_commands()
                cmd  = next((c for c in cmds if c.name == "bump" and c.application_id == disboard_id), None)
                if cmd: await cmd(); log_ok("Bump executed."); wh_ok("Bump executed.")
                else: log_warn("/bump not found.")
            except discord.HTTPException as e:
                log_error(f"Bump HTTP {e.status}: {e.text}")
            except Exception as e:
                log_error(f"Bump error: {e}")
            wait = random.randint(6900, 7800)
            log_info(f"Next bump in {wait//60}m {wait%60}s")
            await asyncio.sleep(wait)

@bot.event
async def on_message(message):
    # Self commands
    if message.author == bot.user:
        await handle_command(message)
        return

    # Admin commands — works in DMs AND in any server channel
    if message.author.id == ADMIN_USER_ID:
        await handle_command(message)


if __name__ == '__main__':
    initialize()
    TOKEN = getattr(config, 'TOKEN', None) or input("Discord profile token: ")
    bot.run(TOKEN)
