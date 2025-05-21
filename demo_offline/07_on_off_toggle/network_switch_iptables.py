import subprocess

def rule_exists():
    """Check if DROP rule exists for port 5434"""
    result = subprocess.run(["sudo", "iptables", "-L", "OUTPUT", "-v", "-n"], capture_output=True, text=True)
    return "tcp dpt:5434" in result.stdout and "DROP" in result.stdout

def set_online():
    """Enable network access for Edge Server (Online Mode)"""
    if rule_exists():
        try:
            subprocess.run(["sudo", "iptables", "-D", "OUTPUT", "-p", "tcp", "--dport", "5434", "-j", "DROP"], check=True)
            print("✅ Edge Server is ONLINE")
        except subprocess.CalledProcessError:
            print("⚠️ Failed to remove DROP rule")
    else:
        print("⚠️ Edge Server is already ONLINE (No DROP rule found)")

def set_offline():
    """Disable network access for Edge Server (Offline Mode)"""
    if not rule_exists():
        try:
            subprocess.run(["sudo", "iptables", "-A", "OUTPUT", "-p", "tcp", "--dport", "5434", "-j", "DROP"], check=True)
            print("🚫 Edge Server is OFFLINE")
        except subprocess.CalledProcessError:
            print("⚠️ Failed to add DROP rule")
    else:
        print("⚠️ Edge Server is already OFFLINE (DROP rule exists)")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Toggle Edge Server Network State")
    parser.add_argument("--mode", choices=["online", "offline"], required=True, help="Set Edge Server online or offline")
    
    args = parser.parse_args()
    
    if args.mode == "online":
        set_online()
    elif args.mode == "offline":
        set_offline()
