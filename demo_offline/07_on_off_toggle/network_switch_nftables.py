import subprocess

def set_online():
    """Enable network access using nftables (Online Mode)"""
    try:
        subprocess.run(["sudo", "nft", "delete", "rule", "inet", "filter", "output", "tcp", "dport", "5434", "drop"], check=True)
        print("✅ Edge Server is ONLINE")
    except subprocess.CalledProcessError:
        print("⚠️ Edge Server is already ONLINE or rule does not exist")

def set_offline():
    """Disable network access using nftables (Offline Mode)"""
    try:
        subprocess.run(["sudo", "nft", "add", "rule", "inet", "filter", "output", "tcp", "dport", "5434", "drop"], check=True)
        print("🚫 Edge Server is OFFLINE")
    except subprocess.CalledProcessError:
        print("⚠️ Edge Server is already OFFLINE")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Toggle Edge Server Network State")
    parser.add_argument("--mode", choices=["online", "offline"], required=True, help="Set Edge Server online or offline")
    
    args = parser.parse_args()
    
    if args.mode == "online":
        set_online()
    elif args.mode == "offline":
        set_offline()
