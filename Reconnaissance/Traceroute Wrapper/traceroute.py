import argparse
import socket
import sys
import os
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any


# ------------------------------------------------------------------
# Optional Dependencies
# ------------------------------------------------------------------
try:
    from scapy.layers.inet import IP, UDP, TCP, ICMP
    from scapy.sendrecv import sr1, send
    from scapy.config import conf
    conf.verb = 0
    HAS_SCAPY = True
except ImportError:
    HAS_SCAPY = False


# ------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------
def is_admin() -> bool:
    try:
        if os.name == 'nt':
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin() != 0
        getuid = getattr(os, "getuid", None)
        return getuid() == 0 if getuid else False
    except Exception:
        return False


def resolve_target(target: str) -> Optional[str]:
    if not target:
        return None
    try:
        return socket.gethostbyname(target)
    except socket.gaierror:
        return None


def get_hostname(ip: Optional[str]) -> str:
    if not ip or ip == "*":
        return ""
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror, IndexError):
        return ""


def get_as_info(ip: Optional[str]) -> str:
    """
    Retrieve ASN information for a given IP using Team Cymru's DNS service.
    """
    if not ip or ip == "*" or ip.startswith("192.168.") or ip.startswith("10."):
        return ""
    try:
        reversed_ip = ".".join(reversed(ip.split(".")))
        query = f"{reversed_ip}.origin.asn.cymru.com"
        # DNS TXT record lookup using standard library
        asn_data = socket.gethostbyname_ex(query)[0]
        if asn_data:
            return f"AS{asn_data.split('.')[0]}"
    except Exception:
        pass
    return ""


# ------------------------------------------------------------------
# Core Traceroute Logic
# ------------------------------------------------------------------
def probe_hop(target_ip: str, ttl: int, proto: str, timeout: float, port: int = 80) -> Dict[str, Any]:
    rtts = []
    hop_addr = None
    
    src_port = random.randint(40000, 65000)
    ip_layer = IP(dst=target_ip, ttl=ttl)
    
    if proto.upper() == "ICMP":
        pkt = ip_layer / ICMP(id=src_port)
    elif proto.upper() == "TCP":
        pkt = ip_layer / TCP(sport=src_port, dport=port, flags="S")
    else: 
        pkt = ip_layer / UDP(sport=src_port, dport=33434)

    for _ in range(3):
        start_time = time.perf_counter()
        resp = sr1(pkt, timeout=timeout, verbose=0)
        end_time = time.perf_counter()
        
        if resp:
            hop_addr = resp.src
            rtts.append((end_time - start_time) * 1000)
            
            if proto.upper() == "TCP" and resp.haslayer(TCP) and resp.src == target_ip:
                rst = IP(dst=target_ip) / TCP(sport=src_port, dport=port, flags="R")
                send(rst, verbose=0)
        else:
            rtts.append(None)

    valid_rtts = [r for r in rtts if r is not None]
    avg_ms = sum(valid_rtts) / len(valid_rtts) if valid_rtts else None
    
    return {
        "ttl": ttl,
        "ip": hop_addr if hop_addr else "*",
        "hostname": get_hostname(hop_addr),
        "as_info": get_as_info(hop_addr),
        "avg_ms": avg_ms
    }


# ------------------------------------------------------------------
# Main Execution
# ------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Vigilo Traceroute Wrapper")
    parser.add_argument("target", nargs="?", help="Target IP or Hostname")
    parser.add_argument("-m", "--max-hops", type=int, default=30, help="Max hops")
    parser.add_argument("-t", "--timeout", type=float, default=2.0, help="Timeout in seconds")
    parser.add_argument("-p", "--protocol", choices=["ICMP", "UDP", "TCP"], default="ICMP", help="Probe protocol")
    parser.add_argument("-w", "--workers", type=int, default=10, help="Parallel workers")
    parser.add_argument("-o", "--output", help="Save results to JSON file")
    
    args = parser.parse_args()

    if not HAS_SCAPY:
        print("[!] Error: 'scapy' library required.")
        sys.exit(1)
    if not is_admin():
        print("[!] Error: Administrative privileges required.")
        sys.exit(1)

    print("--- Vigilo Traceroute Wrapper ---")

    target_ip = resolve_target(args.target)
    if not target_ip:
        while not target_ip:
            raw = input("Enter target IP/Hostname: ").strip()
            if not raw:
                continue
            target_ip = resolve_target(raw)
            if not target_ip:
                print("[!] Could not resolve.")
        
        mode_sel = input("Choose Protocol [1=ICMP, 2=UDP, 3=TCP] (default=1): ").strip()
        args.protocol = {"2": "UDP", "3": "TCP"}.get(mode_sel, "ICMP")

    print(f"[*] Path to {target_ip} using {args.protocol} probes.")
    print(f"{'Hop':<4} {'IP Address':<16} {'ASN':<10} {'Hostname/Organization'}")
    print("-" * 85)

    hop_results = {}
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all probes immediately for parallel speed
        futures = {executor.submit(probe_hop, target_ip, ttl, args.protocol, args.timeout): ttl 
                    for ttl in range(1, args.max_hops + 1)}
        
        target_reached = False
        reached_ttl = args.max_hops

        for ttl in range(1, args.max_hops + 1):
            current_future = next(f for f, t in futures.items() if t == ttl)
            try:
                hop_data = current_future.result()
                hop_results[ttl] = hop_data
                
                avg_str = f"{hop_data['avg_ms']:.2f} ms" if hop_data['avg_ms'] else "*"
                host_info = hop_data['hostname'] or "No RDNS"
                as_info = hop_data['as_info'] if hop_data['as_info'] else "-"
                
                print(f"{ttl:<4} {hop_data['ip']:<16} {as_info:<10} {host_info:<35} {avg_str}")
                
                if hop_data['ip'] == target_ip:
                    target_reached = True
                    reached_ttl = ttl
                    break
            except KeyboardInterrupt:
                break

    if target_reached:
        print(f"\n[*] Target reached in {reached_ttl} hops.")

    if args.output:
        try:
            with open(args.output, 'w') as f:
                json.dump({"target": target_ip, "protocol": args.protocol, "hops": list(hop_results.values())}, f, indent=4)
            print(f"[*] Results saved to {args.output}")
        except IOError as e:
            print(f"[!] Error saving file: {e}")


# ------------------------------------------------------------------
if __name__ == "__main__":
    main()