import argparse
import socket
import sys
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional


# ------------------------------------------------------------------
# Constants & Configuration
# ------------------------------------------------------------------
# Define the large list once to be memory efficient
ALL_PORTS = list(range(1, 65536))

PORT_PROFILES = {
    "all": ALL_PORTS,
    "*": ALL_PORTS,
    "-": ALL_PORTS,
    "common": [21, 22, 23, 25, 53, 80, 110, 111, 135, 139, 143, 443, 445, 993, 995, 1723, 3306, 3389, 5900, 8080],
    "web": [80, 443, 8080, 8443, 8000, 8008, 8888],
    "db": [1433, 1521, 3306, 5432, 6379, 27017],
    "ssh": [22, 2222, 2200],
    "windows": [135, 139, 445, 3389, 5985, 49668]
}

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

try:
    from scapy.layers.inet import IP, TCP
    from scapy.sendrecv import sr1, send
    from scapy.config import conf
    conf.verb = 0
    HAS_SCAPY = True
except ImportError:
    HAS_SCAPY = False


# ------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------
def resolve_target(target: str) -> Optional[str]:
    if not target:
        return None
    try:
        return socket.gethostbyname(target)
    except socket.gaierror:
        return None

def parse_ports(ports_str: str) -> List[int]:
    """
    Parses a string of ports, ranges, or profiles into a list of port numbers.
    """
    clean_str = ports_str.lower().strip()

    if clean_str in PORT_PROFILES:
        # Use set to avoid duplicates in profiles
        return sorted(list(set(PORT_PROFILES[clean_str])))

    ports = set()
    parts = ports_str.split(',')
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                ports.update(range(start, end + 1))
            except ValueError:
                continue
        else:
            try:
                ports.add(int(part))
            except ValueError:
                continue
    return sorted(list(ports))


def is_admin() -> bool:
    try:
        is_admin_flag = False
        getuid = getattr(os, "getuid", None)

        if getuid:
            is_admin_flag = (getuid() == 0)
        elif os.name == 'nt':
            import ctypes
            is_admin_flag = (ctypes.windll.shell32.IsUserAnAdmin() != 0)
            
        return is_admin_flag
    except Exception:
        return False


def guess_os(ttl: int) -> str:
    if ttl <= 64:
        return "Linux/Unix"
    elif ttl <= 128:
        return "Windows"
    elif ttl <= 255:
        return "Cisco/Net"
    return "Unknown"


# ------------------------------------------------------------------
# Core Scanning Logic
# ------------------------------------------------------------------
def grab_banner_from_socket(s: socket.socket) -> str:
    try:
        # Phase 1: Passive Listen
        try:
            response = s.recv(1024)
            if response:
                return response.decode('utf-8', errors='ignore').split('\r\n')[0].strip()
        except socket.timeout:
            pass 

        # Phase 2: Active Trigger
        trigger = b"HEAD / HTTP/1.0\r\n\r\n"
        try:
            s.sendall(trigger)
            response = s.recv(1024)
            if response:
                return response.decode('utf-8', errors='ignore').split('\r\n')[0].strip()
        except (socket.timeout, socket.error):
            pass
        
        return ""
    except Exception:
        return ""


def scan_connect(ip: str, port: int, timeout: float, retries: int) -> Tuple[int, bool, str, str]:
    for _ in range(retries + 1):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                if s.connect_ex((ip, port)) == 0:
                    banner = grab_banner_from_socket(s)
                    return port, True, banner, "-"
        except OSError:
            pass
    return port, False, "", ""


def scan_syn(ip: str, port: int, timeout: float, retries: int) -> Tuple[int, bool, str, str]:
    if not HAS_SCAPY:
        return port, False, "Error: Scapy missing", ""

    for _ in range(retries + 1):
        try:
            pkt = IP(dst=ip) / TCP(dport=port, flags="S")
            resp = sr1(pkt, timeout=timeout, verbose=0)
            
            if resp and resp.haslayer(TCP):
                tcp_layer = resp.getlayer(TCP)
                if tcp_layer:
                    flags = getattr(tcp_layer, "flags", 0)
                    
                    if flags == 0x12: # SYN+ACK
                        os_guess = "Unknown"
                        ip_layer = resp.getlayer(IP)
                        if ip_layer:
                            ttl = getattr(ip_layer, "ttl", 0)
                            os_guess = guess_os(ttl)

                        rst_pkt = IP(dst=ip) / TCP(dport=port, flags="R")
                        send(rst_pkt, verbose=0)
                        
                        # Attempt banner grab via standard socket
                        banner = ""
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                s.settimeout(timeout)
                                s.connect((ip, port))
                                banner = grab_banner_from_socket(s)
                        except Exception:
                            pass

                        return port, True, banner, os_guess
                    
                    elif flags == 0x14: # RST+ACK
                        return port, False, "", ""
        except Exception:
            pass
            
    return port, False, "", ""


# ------------------------------------------------------------------
# Main Execution
# ------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Professional TCP Scanner")
    parser.add_argument("target", nargs="?", help="Target IP or Hostname")
    parser.add_argument("-p", "--ports", help="Ports, Profile, or 'all'")
    parser.add_argument("-t", "--timeout", type=float, default=1.5, help="Timeout in seconds")
    parser.add_argument("-w", "--workers", type=int, default=50, help="Worker threads")
    parser.add_argument("-r", "--retries", type=int, default=0, help="Retries per port")
    parser.add_argument("--syn", action="store_true", help="Use SYN Scan (Root required)")
    parser.add_argument("-o", "--output", help="Save results to JSON file")
    
    args = parser.parse_args()
    
    print("--- Python TCP Scanner v3.1 ---")

    # 1. Setup
    target_input = args.target
    target_ip = None
    scan_mode = "CONNECT" # Default
    admin_privs = is_admin()

    # Determine Mode from CLI flags first
    if args.syn:
        if not HAS_SCAPY:
            print("[!] Error: SYN scan requires 'scapy' library.")
            sys.exit(1)
        if not admin_privs:
            print("[!] Error: SYN scan requires Root/Admin privileges.")
            sys.exit(1)
        scan_mode = "SYN"

    # 2. Interactive Input
    if not target_input:
        while not target_ip:
            raw = input("Enter target IP/Hostname: ").strip()
            if not raw:
                print("[!] Please enter a valid IP or Hostname.")
                continue
            target_ip = resolve_target(raw)
            if not target_ip:
                print("[!] Could not resolve. Try again.")
        
        print(f"Profiles available: {', '.join(PORT_PROFILES.keys())}")
        print("Options: 'all' (1-65535), ranges '1-100', or lists '80,443'")
        raw_ports = input("Enter ports or profile (default=common): ").strip()
        ports = parse_ports(raw_ports) if raw_ports else parse_ports("common")

        # Scan Type Selection Logic
        print("\nSelect Scan Type:")
        print(" 1. TCP Connect (Standard, Non-privileged)")
        
        syn_status = ""
        if not HAS_SCAPY:
            syn_status = "[Disabled - Scapy not found]"
        elif not admin_privs:
            syn_status = "[Disabled - Requires Admin/Root]"
        else:
            syn_status = "[Available]"
            
        print(f" 2. TCP SYN (Stealth, OS Fingerprint) {syn_status}")
        
        mode_sel = input("Choice [default=1]: ").strip()
        if mode_sel == "2":
            if "Disabled" in syn_status:
                print(f"[!] Cannot run SYN scan: {syn_status}. Falling back to Connect.")
            else:
                scan_mode = "SYN"

    else:
        # CLI Argument Processing
        target_ip = resolve_target(target_input)
        if not target_ip:
            print(f"[!] Error: Could not resolve '{target_input}'")
            sys.exit(1)
        
        ports = parse_ports(args.ports) if args.ports else parse_ports("common")
    
    if not ports:
        print("[!] Error: No ports defined.")
        sys.exit(1)

    # 3. Execution
    if len(ports) > 1000:
        print(f"[!] Warning: Scanning {len(ports)} ports. This may take time.")

    print(f"\n[*] Target: {target_ip}")
    print(f"[*] Mode: {scan_mode} | Ports: {len(ports)} | Threads: {args.workers}")
    
    open_ports = []
    
    pbar = tqdm(total=len(ports), unit="port", ncols=80) if HAS_TQDM else None
    scan_func = scan_syn if scan_mode == "SYN" else scan_connect
    
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(scan_func, target_ip, port, args.timeout, args.retries): port 
                for port in ports
            }
            
            for future in as_completed(futures):
                port_num, is_open, banner, os_guess = future.result()
                
                if is_open:
                    open_ports.append({
                        'port': port_num, 
                        'banner': banner, 
                        'os_guess': os_guess
                    })
                    
                    status_symbol = "[+]"
                    status_text = "OPEN"
                    
                    if not banner and scan_mode == "CONNECT":
                        status_symbol = "[?]"
                        status_text = "OPEN (Silent)"
                    
                    msg = f"{status_symbol} Port {port_num:<5} {status_text}"
                    
                    if scan_mode == "SYN":
                        msg += f" (OS: {os_guess})"
                    elif banner:
                        msg += f" : {banner[:30]}..."
                    
                    if pbar:
                        pbar.write(msg)
                    else:
                        print(msg)
                
                if pbar:
                    pbar.update(1)
                    
    except KeyboardInterrupt:
        print("\n[!] Scan aborted by user.")
        if pbar:
            pbar.close()
        sys.exit(0)
    finally:
        if pbar:
            pbar.close()

    # 4. Results
    print("\n" + "="*60)
    print("SCAN COMPLETE")
    print(f"Open: {len(open_ports)} | Closed/Filtered: {len(ports) - len(open_ports)}")
    print("="*60)
    
    open_ports.sort(key=lambda x: x['port'])
    silent_count = sum(1 for p in open_ports if not p['banner'])
    
    if open_ports:
        print(f"{'PORT':<8} {'OS GUESS':<15} {'SERVICE/BANNER'}")
        print("-" * 60)
        for item in open_ports:
            banner_disp = item['banner'] if item['banner'] else "[Silent/Unknown]"
            print(f"{item['port']:<8} {item['os_guess']:<15} {banner_disp}")
            
        if len(open_ports) > 5 and silent_count > (len(open_ports) / 2):
            print("-" * 60)
            print("[!] WARNING: High number of silent open ports detected.")
            print("    This usually indicates a FIREWALL, PROXY, or ANTIVIRUS")
            print("    is intercepting connections (False Positives).")
    else:
        print("No open ports found.")

    if args.output:
        try:
            with open(args.output, 'w') as f:
                json.dump({
                    "target": target_ip,
                    "mode": scan_mode,
                    "results": open_ports
                }, f, indent=4)
            print(f"\n[*] Results saved to {args.output}")
        except IOError as e:
            print(f"\n[!] Error saving file: {e}")


# ------------------------------------------------------------------
if __name__ == "__main__":
    main()