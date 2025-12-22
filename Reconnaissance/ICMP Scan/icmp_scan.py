import ipaddress
import subprocess
import time
import re
import platform
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


# determine OS
is_windows = platform.system().lower().startswith("win")

# detect local network
if is_windows:
        # windows: use ipconfig
        win_out = subprocess.check_output(["ipconfig"]).decode(errors="ignore")
        addr = mask = None
        for line in win_out.splitlines():
                line = line.strip()
                if line.startswith("IPv4 Address"):
                        addr = line.split(":", 1)[1].strip()
                elif line.startswith("Subnet Mask"):
                        mask = line.split(":", 1)[1].strip()
                if addr and mask:
                        break
        if not (addr and mask):
                print("ERROR: Could not detect IPv4 Address/Mask from ipconfig.")
                sys.exit(1)
        prefix = ipaddress.IPv4Network(f"0.0.0.0/{mask}", strict=False).prefixlen
        detected_cidr = f"{addr}/{prefix}"
else:
        # linux/macOS: use ip command
        try:
                output = subprocess.check_output(["ip", "-o", "-f", "inet", "addr", "show"]).decode()
        except subprocess.CalledProcessError:
                print("ERROR: Unable to run 'ip' command.")
                sys.exit(1)
        for line in output.splitlines():
                if "127.0.0.1" in line:
                        continue
                match = re.search(r"inet (\d+\.\d+\.\d+\.\d+)/(\d+)", line)
                if match:
                        detected_cidr = f"{match.group(1)}/{match.group(2)}"
                        break
        else:
                print("ERROR: No valid non-loopback IPv4 interface found.")
                sys.exit(1)
print(f"Detected local network: {detected_cidr}")

detected_network = ipaddress.IPv4Network(detected_cidr, strict=False)


# prompt for scan parameters (blank = default)
raw = input("Packet count per host [default=1]: ").strip()
count = int(raw) if raw else 1

raw = input("Timeout in seconds [default=1.0]: ").strip()
timeout = float(raw) if raw else 1.0

raw = input("Worker threads [default=20]: ").strip()
workers = int(raw) if raw else 20

print(f"\nUsing count={count}, timeout={timeout}s, workers={workers}\n")


# prompt user and enforce their input matches detected network
while True:
        prompt = (f"Enter network CIDR to scan (detected: {detected_network}): ")
        user_input = input(prompt).strip()
        if "/" not in user_input:
                print("You must include a subnet mask (e.g. /24.)\n")
                continue
        try:
                user_net = ipaddress.IPv4Network(user_input, strict=False)
        except ValueError:
                print("Invalid CIDR format.\n")
                continue
        if (user_net.network_address != detected_network.network_address
            or user_net.prefixlen != detected_network.prefixlen):
                print(f"Input does not match detected network {detected_network}\n")
                continue
        network = user_net
        break

hosts = list(network.hosts())
print(f"Scanning {network} ({network.num_addresses} hosts)\n")


# ICMP ping function with RTT measurement
def ping_hosts(ip: str) -> tuple[bool, float]:
        if is_windows:
                args = ["ping", "-n", str(count), "-w", str(int(timeout*1000)), ip]
        else:
                args = ["ping", "-c", str(count), "-W", str(int(timeout)), ip]
        start = time.monotonic()
        result = subprocess.run(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
        )
        end = time.monotonic()
        return (result.returncode == 0, (end - start) * 1000)


# perform parallel scan with progress bar
active_hosts = []
with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(ping_hosts, str(host)): str(host) for host in hosts}
        for future in tqdm(as_completed(futures),
                   total=len(hosts),
                   desc="Scanning hosts",
                   unit="host"):
                ip_str = futures[future]
                is_up, rtt = future.result()
                if is_up:
                        tqdm.write(f"{ip_str} is up ({rtt:.2f} ms RTT)")
                        active_hosts.append((ip_str, rtt))
print("\nScan complete.")