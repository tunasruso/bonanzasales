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
    # - freetds-dev: for pymssql
    # - curl, gnupg: for Tailscale install
    # - gzip: for gost install
    .apt_install("freetds-dev", "gcc", "g++", "make", "curl", "gnupg", "gzip")
    
    # Python dependencies
    .pip_install("pymssql", "requests")
    
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

    # Mount the sync script directly into the image
    .add_local_file("/Users/tunasruso/Documents/Antigravity/StasSales1CBackEnd/sync_to_supabase.py", "/root/sync_to_supabase.py")
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
    # 2. START GOST Tunnel
    # ═══════════════════════════════════════════════════════════════════════════════
    # Map Local 1433 -> Tailscale 100.126.198.90:1433 via SOCKS5 1055
    log.info("🔌 Starting GOST Tunnel (127.0.0.1:1433 -> 100.126.198.90:1433)...")
    
    # gost -L tcp://127.0.0.1:1433/100.126.198.90:1433 -F socks5://127.0.0.1:1055
    gost_cmd = [
        "gost",
        "-L", "tcp://127.0.0.1:1433/100.126.198.90:1433",
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
    # 3. EXECUTE SYNC SCRIPT
    # ═══════════════════════════════════════════════════════════════════════════════
    log.info("🚀 Launching sync logic...")
    
    # Add root to path so we can import the script
    sys.path.append("/root")
    
    try:
        import sync_to_supabase
        
        # MONKEY PATCH CONFIGURATION to use Localhost Tunnel
        log.info("🔧 Patching DB Configuration to use Localhost Tunnel...")
        sync_to_supabase.DB_CONFIG['server'] = '127.0.0.1'
        sync_to_supabase.DB_CONFIG['port'] = 1433 
        
        # START
        sync_to_supabase.main()
        
    except Exception as e:
        log.error(f"❌ Sync failed: {e}")
        raise e
    finally:
        gost_proc.terminate()

@app.local_entrypoint()
def main():
    print("🚀 Triggering remote sync job on Modal...")
    run_sync_job.remote()
