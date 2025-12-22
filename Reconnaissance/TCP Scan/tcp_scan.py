import socket
import ipaddress
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from scapy.layers.inet import IP, TCP
from scapy.sendrecv import sr1
from scapy.config import conf
conf.verb = 0


while True:
    raw = input("Enter target IP: ").strip()
    if re.fullmatch(r"[0-9\.]+", raw):
        try:
            addr = ipaddress.IPv4Address(raw)
            target_ip = str(addr)
            break
        except ipaddress.AddressValueError:
            print(f"Invalid IPv4 address '{raw}'. Please try again.\n")
            continue
    try:
        target_ip = socket.gethostbyname(raw)
        break
    except socket.gaierror:
        print(f"Could not resolve '{raw}'. Please try again.\n")


def parse_ports(ports_raw: str) -> list[int]:
    ports: list[int] = []
    for port in ports_raw.split(","):
        port = port.strip()
        if "-" in port:
            start_str, end_str = port.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            ports.extend(range(start, end + 1))
        else:
            ports.append(int(port))
    return ports


ports_raw = input("Enter ports/port ranges separated by comma "
                  "default=[1-1024]: ").strip()
if not ports_raw:
    # default to ports 1-1024
    ports = list(range(1, 1025))
else:
    try:
        ports = parse_ports(ports_raw)
    except ValueError:
        print("Invalid port specification-falling back to 1-1024.")
        ports = list(range(1, 1025))

raw = input("Timeout in seconds [default=1.0]: ").strip()
timeout = float(raw) if raw else 1.0

raw = input("Worker threads [default=20]: ").strip()
workers = int(raw) if raw else 20

raw = input("Retries per port [default=0]: ").strip()
retries = int(raw) if raw else 0

print("Scan type options:")
print(" 1) TCP Connect scan (full handshake)")
print(" 2) TCP SYN scan (stealth, requires root)")
raw = input("Choose scan type [default=1]: ")
scan_type = int(raw) if raw in ("1", "2") else 1

print(f"\nUsing timeout={timeout}s, workers={workers}, retries-{retries} scan_type={scan_type}\n, ")


def grab_banner(sock) -> str:
    sock.settimeout(1.0)
    try:
        data = sock.recv(1024)
        return data.decode(errors="ignore").strip()
    except (socket.timeout, OSError):
        return ""


def scan_port(port: int) -> tuple[bool, str]:
    for attempt in range(retries + 1):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect((target_ip, port))
            banner = grab_banner(s)
            return True, banner
        except (socket.timeout, ConnectionRefusedError):
            if attempt == retries:
                return False, ""
        finally:
            s.close()
    return False, ""


def scan_port_syn(port: int) -> bool:
    pkt = IP(dst=target_ip)/TCP(dport=port, flags="S")
    resp = sr1(pkt, timeout=timeout)
    if not resp or not resp.haslayer(TCP):
        return False
    
    tcp_layer = resp.getlayer(TCP)
    # flags 0x12 is SYN+ACK
    if isinstance(tcp_layer, TCP) and tcp_layer.flags == 0x12:
        # send RST to close the half-open connection
        rst = IP(dst=target_ip)/TCP(dport=port, flags="R")
        sr1(rst, timeout=timeout)
        return True
    return False


active_ports = []


if scan_type ==1:
    scan_fn = scan_port
else:
    def scan_fn(port: int) -> tuple[bool, str]:
        is_open = scan_port_syn(port)
        return is_open, ""


with ThreadPoolExecutor(max_workers=workers) as executor:
    futures = {executor.submit(scan_fn, port): port for port in ports}
    for future in tqdm(as_completed(futures),
                       total=len(ports),
                       desc="Scanning ports",
                       unit="port"):
        port = futures[future]
        is_open, banner = future.result()
        if is_open:
            msg = f"Port {port} is open"
            if banner:
                msg += f" - banner: {banner}"
            tqdm.write(msg)
            active_ports.append((port, banner))
print("Scan complete.")
print("\nScan complete. Open ports:")
print(f"{'Port':>5} | Banner")
print("-" * 60)
for port, banner in active_ports:
    print(f"{port:>5} | {banner}")