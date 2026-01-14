import argparse
import ipaddress
import platform
import socket
import subprocess
import re
import sys
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, List, Optional, Generator


# ------------------------------------------------------------------
# Optional Dependencies
# ------------------------------------------------------------------
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# ------------------------------------------------------------------
# System Constants & Configuration
# ------------------------------------------------------------------
SYSTEM = platform.system().lower()
IS_WINDOWS = SYSTEM.startswith("win")
IS_MACOS = SYSTEM == "darwin"
IS_LINUX = SYSTEM == "linux"

def get_local_ip() -> str:
    """
    Detects local IP by connecting to a public DNS (no data sent).
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Use a non-routable IP to avoid sending data
        s.connect(("10.255.255.255", 1))
        local_ip = s.getsockname()[0]
    except Exception:
        local_ip = "127.0.0.1"
    finally:
        s.close()
    return local_ip

def parse_ping_rtt(output: str) -> float:
    """
    Parses the RTT from ping output across different OS locales.
    """
    # Regex to catch: "time=12ms", "time<1ms", "Zeit=12ms"
    match = re.search(r"(?:time|Zeit|temps|durée)[=<]([\d\.]+)\s*ms", output, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
            
    # Fallback for generic formats
    if "TTL=" in output.upper():
        match_generic = re.search(r"([\d\.]+)\s*ms", output)
        if match_generic:
            try:
                return float(match_generic.group(1))
            except ValueError:
                pass
    return 0.0


def ping_host(ip: str, count: int, timeout: float) -> Tuple[str, bool, float]:
    """
    Pings a single host and returns its status and RTT.
    """
    # Construct ping command based on OS
    if IS_WINDOWS:
        cmd = ["ping", "-n", str(count), "-w", str(int(timeout * 1000)), ip]
        env = None
    elif IS_MACOS:
        cmd = ["ping", "-c", str(count), "-W", str(int(timeout * 1000)), ip]
        env = {"LC_ALL": "C", "PATH": os.environ.get("PATH", "/sbin:/usr/sbin:/bin:/usr/bin")}
    else:
        cmd = ["ping", "-c", str(count), "-W", str(timeout), ip]
        env = {"LC_ALL": "C", "PATH": os.environ.get("PATH", "/sbin:/usr/sbin:/bin:/usr/bin")}

    # Prepare subprocess arguments
    subprocess_args = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.DEVNULL,
        "text": True,
        "env": env,
        "errors": "ignore"
    }
    
    if IS_WINDOWS:
        # creationflags=0x08000000 suppresses the console window popping up on Windows 
        subprocess_args["creationflags"] = 0x08000000

    try:
        result = subprocess.run(cmd, **subprocess_args)
        
        is_up = (result.returncode == 0)
        rtt = parse_ping_rtt(result.stdout) if is_up else 0.0
        return ip, is_up, rtt
    except Exception:
        return ip, False, 0.0


def validate_cidr(cidr: str) -> Optional[ipaddress.IPv4Network]:
    try:
        network = ipaddress.IPv4Network(cidr, strict=False)
        return network
    except ValueError:
        return None


def batch_hosts(network: ipaddress.IPv4Network, batch_size: int = 1000) -> Generator[List[str], None, None]:
    """
    Yields hosts in batches to prevent memory exhaustion on large networks.
    """
    batch = []
    for host in network.hosts():
        batch.append(str(host))
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


# ------------------------------------------------------------------
# Main Logic
# ------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Professional ICMP Scanner (Portable Mode)")
    parser.add_argument("network", nargs="?", help="CIDR to scan (e.g., 192.168.1.0/24)")
    parser.add_argument("-c", "--count", type=int, default=1, help="Packets per host (Default: 1)")
    parser.add_argument("-t", "--timeout", type=float, default=1.0, help="Timeout in seconds (Default: 1.0)")
    parser.add_argument("-w", "--workers", type=int, default=50, help="Worker threads (Default: 50)")
    parser.add_argument("-o", "--output", help="Save results to JSON file")
    
    args = parser.parse_args()
    
    local_ip = get_local_ip()
    print(f"--- Python ICMP Scanner (Local IP: {local_ip}) ---")

    target_cidr = args.network
    network = None

    # CIDR Validation
    if target_cidr:
        # Non-Interactive Mode
        network = validate_cidr(target_cidr)
        if not network:
            print(f"[!] Error: '{target_cidr}' is not a valid CIDR.")
            sys.exit(1)
    else:
        # Interactive Mode: Prompt for all inputs
        suggested = f"{local_ip.rsplit('.', 1)[0]}.0/24"
        while not network:
            target_cidr = input(f"Enter network to scan [default={suggested}]: ").strip() or suggested
            network = validate_cidr(target_cidr)
            if not network:
                print("[!] Invalid CIDR. Try again.")
        # Additional Prompts for Scan Parameters
        try:
            val = input(f"Packets per host [default={args.count}]: ").strip()
            if val:
                args.count = int(val)

            val = input(f"Timeout (s) [default={args.timeout}]: ").strip()
            if val:
                args.timeout = float(val)

            val = input(f"Worker threads [default={args.workers}]: ").strip()
            if val:
                args.workers = int(val)
        except ValueError:
            print("[!] Invalid input detected. Reverting to defaults.")

    # Large Network Confirmation
    if network.num_addresses > 2000 and not args.network:
        confirm = input(f"[!] Network has {network.num_addresses} hosts. Continue? (y/n): ").lower()
        if confirm != 'y':
            sys.exit(0)

    print(f"\n[*] Scanning {network}")
    print(f"[*] Config: {args.workers} workers, {args.count} pkts/host, {args.timeout}s timeout")
    
    active_hosts = []
    
    # Progress Bar Logic
    total_hosts = network.num_addresses - 2 if network.prefixlen < 31 else network.num_addresses
    pbar = tqdm(total=total_hosts, unit="host", ncols=80) if HAS_TQDM else None

    # Threaded Scanning
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Process hosts in batches to manage memory usage
        for batch in batch_hosts(network):
            futures = {executor.submit(ping_host, ip, args.count, args.timeout): ip for ip in batch}
            
            for future in as_completed(futures):
                ip, is_up, rtt = future.result()
                
                if is_up:
                    active_hosts.append((ip, rtt))
                    msg = f"[+] {ip:<15} UP ({rtt:.2f}ms)"
                    if pbar:
                        pbar.write(msg)
                    else:
                        print(msg)
                
                if pbar:
                    pbar.update(1)

    if pbar:
        pbar.close()

    # Results Summary
    print("\n" + "="*35)
    print(f"SCAN COMPLETE: {len(active_hosts)} Hosts Up")
    print("="*35)
    
    active_hosts.sort(key=lambda x: ipaddress.IPv4Address(x[0]))
    
    for ip, rtt in active_hosts:
        print(f"{ip:<15} : {rtt:.2f} ms")

    if args.output:
        try:
            with open(args.output, 'w') as f:
                json.dump([{'ip': ip, 'rtt': rtt} for ip, rtt in active_hosts], f, indent=4)
            print(f"\n[*] Results saved to {args.output}")
        except IOError as e:
            print(f"\n[!] Error saving to file: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[!] Scan aborted by user.")
        sys.exit(0)