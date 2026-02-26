import modal
import sys
import logging
import subprocess
import os
import time

# Define the image with dependencies
image = (
    modal.Image.debian_slim()
    # Install dependencies:
    # - curl, gnupg: for Tailscale install
    # - gzip: for gost install
    .apt_install("curl", "gnupg", "gzip")
    
    # Python dependencies
    .pip_install("psycopg2-binary", "requests", "pandas", "numpy")
    
    # Install Tailscale
    .run_commands(
        "mkdir -p --mode=0755 /usr/share/keyrings",
        "curl -fsSL https://pkgs.tailscale.com/stable/debian/bookworm.noarmor.gpg | tee /usr/share/keyrings/tailscale-archive-keyring.gpg >/dev/null",
        "curl -fsSL https://pkgs.tailscale.com/stable/debian/bookworm.tailscale-keyring.list | tee /etc/apt/sources.list.d/tailscale.list",
        "apt-get update && apt-get install -y tailscale"
    )
    
    # Install GOST (Tunnel)
    .run_commands(
        "curl -sL https://github.com/ginuerzh/gost/releases/download/v2.11.1/gost-linux-amd64-2.11.1.gz | gunzip > /usr/local/bin/gost",
        "chmod +x /usr/local/bin/gost"
    )

    # Mount the verified sync scripts
    .add_local_file("/Users/tunasruso/Documents/Antigravity/StasSales1CBackEnd/custom_inventory_sync.py", "/root/custom_inventory_sync.py")
    .add_local_file("/Users/tunasruso/Documents/Antigravity/StasSales1CBackEnd/sync_to_supabase.py", "/root/sync_to_supabase.py")
    .add_local_file("/Users/tunasruso/Documents/Antigravity/StasSales1CBackEnd/sync_visitors.py", "/root/sync_visitors.py")
)

# App definition with Secret
app = modal.App(
    "bonanza-sales-sync", 
    image=image, 
    secrets=[modal.Secret.from_name("tailscale-auth")]
)

# Schedule: Every hour from 09:00 MSK to 21:00 MSK
# MSK is UTC+3.
# 09:00 MSK = 06:00 UTC
# 21:00 MSK = 18:00 UTC
# Cron range: 6-18
@app.function(timeout=3600, schedule=modal.Cron("0 6-18 * * *"))
def run_sync_job():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("modal_runner")
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # 1. START TAILSCALE
    # ═══════════════════════════════════════════════════════════════════════════════
    log.info("🔌 Starting Tailscale...")
    auth_key = os.environ["TAILSCALE_AUTHKEY"]
    
    # Start tailscaled in background (userspace networking mode, with SOCKS5 on port 1055)
    subprocess.Popen(
        ["tailscaled", "--tun=userspace-networking", "--socks5-server=localhost:1055"], 
        stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL
    )
    time.sleep(3) # Allow daemon to start
    
    # Authenticate and bring interface up
    try:
        cmd = [
            "tailscale", "up",
            f"--authkey={auth_key}",
            "--hostname=modal-worker",
            "--ssh" 
        ]
        subprocess.run(cmd, check=True)
        log.info("✅ Tailscale connected!")
    except subprocess.CalledProcessError as e:
        log.error(f"❌ Tailscale failed to start: {e}")
        raise e

    # ═══════════════════════════════════════════════════════════════════════════════
    # 2. START GOST Tunnel for PostgreSQL
    # ═══════════════════════════════════════════════════════════════════════════════
    # Map Local 5432 -> Tailscale 100.91.185.91:5444 via SOCKS5 1055
    log.info("🔌 Starting GOST Tunnel (127.0.0.1:5432 -> 100.91.185.91:5444)...")
    
    gost_cmd = [
        "gost",
        "-L", "tcp://127.0.0.1:5432/100.91.185.91:5444",
        "-F", "socks5://127.0.0.1:1055"
    ]
    
    # Let gost log to stdout
    gost_proc = subprocess.Popen(gost_cmd, stdout=sys.stdout, stderr=sys.stderr)
    time.sleep(3) # Stabilize
    
    if gost_proc.poll() is not None:
         log.error("❌ GOST process failed immediately")
         raise Exception("GOST failed")
         
    log.info("✅ GOST Tunnel running")
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # 3. EXECUTE SYNC LOGIC
    # ═══════════════════════════════════════════════════════════════════════════════
    log.info("🚀 Launching sync logic...")
    
    # Add root to path
    sys.path.append("/root")
    
    # INJECT CREDENTIALS
    os.environ['POSTGRES_HOST'] = '127.0.0.1' # Local tunnel end
    os.environ['POSTGRES_PORT'] = '5432'      # Local tunnel end
    os.environ['POSTGRES_USER'] = 'ecostock'
    os.environ['POSTGRES_PASSWORD'] = 'Kd*2m5Th'
    os.environ['POSTGRES_DB'] = 'onec_ecostock_retail'
    
    try:
        import custom_inventory_sync
        import sync_to_supabase
        import sync_visitors
        
        # --- JOB 1: INVENTORY SYNC ---
        log.info("📦 [1/3] Syncing INVENTORY...")
        custom_inventory_sync.upload_to_supabase(custom_inventory_sync.extract_inventory())
        log.info("✅ Inventory sync completed.")

        # --- JOB 2: SALES SYNC ---
        log.info("💰 [2/3] Syncing SALES...")
        sync_to_supabase.main()
        log.info("✅ Sales sync completed.")

        # --- JOB 3: VISITORS (TRAFFIC) SYNC ---
        log.info("🚶 [3/3] Syncing VISITORS...")
        sync_visitors.main()
        log.info("✅ Visitors sync completed.")
        
    except Exception as e:
        log.error(f"❌ Sync failed: {e}")
        raise e
    finally:
        gost_proc.terminate()

@app.local_entrypoint()
def main():
    print("🚀 Triggering remote sync job on Modal...")
    run_sync_job.remote()
