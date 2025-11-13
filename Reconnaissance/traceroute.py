import socket
import ipaddress
import re
import time
from scapy.layers.inet import IP, TCP, ICMP
from scapy.sendrecv import sr1
from scapy.config import conf
conf.verb = 0


# resolve target
while True:
    raw = input("Enter target IP or hostname: ").strip()
    if re.fullmatch(r"[0-9\.]+", raw):
        try:
            addr = ipaddress.IPv4Address(raw)
            target = str(addr)
            break
        except ipaddress.AddressValueError:
            print(f"Invalid IPv4 address '{raw}'. Please try again.\n")
            continue
    try:
        target = socket.gethostbyname(raw)
        break
    except socket.gaierror:
        print(f"Could not resolve '{raw}'. Please try again.\n")


# prompt for traceroute parameters
raw = input("Max hops [default=30]: ").strip()
max_hops = int(raw) if raw.isdigit() else 30

raw = input("Timeout per probe in seconds [default=2.0]: ").strip()
timeout = float(raw) if raw else 2.0

print(f"\nTracerouting to {target} with max_hops={max_hops}, timeout={timeout}s\n")

# print table header
ttl_w  = 3
ip_w   = 15
host_w = 40

header = f"{'TTL':>{ttl_w}}  {'IP':<{ip_w}}  {'Hostname':<{host_w}}  {'Min/Avg/Max Latency'}"
print(header)
print("-" * len(header))


for ttl in range(1, max_hops + 1):
    # prepare packet for hop
    pkt = IP(dst=target, ttl=ttl) / ICMP()
    
    probes = 5
    rtts = []
    hop_ip = None
    
    # send multiple probes per hop
    for _ in range(probes):
        start = time.time()
        resp = sr1(pkt, timeout=timeout)
        elapsed = (time.time() - start) * 1000 # time in ms
        if resp:
            hop_ip = resp.src
            rtts.append(elapsed)
        else:
            rtts.append(None)
    
    # compute stats
    valid = [r for r in rtts if r is not None]
    if valid:
        mn = min(valid)
        mx = max(valid)
        avg = sum(valid) / len(valid)
        stats = f"{mn:.1f}/{avg:.1f}/{mx:.1f} ms"
    else:
        stats = "*"
    
    # perform reverse DNS if we got an IP
    name = ""
    if hop_ip:
        try:
            name = socket.gethostbyaddr(hop_ip)[0]
        except socket.herror:
            pass
    
    # realtime output
    ip_display = hop_ip or "*"
    print(f"{ttl:>{ttl_w}}  {ip_display:<{ip_w}}  {name:<{host_w}}  {stats}")
    
    if hop_ip == target:
        break
print("\nTraceroute complete.")