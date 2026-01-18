import socket


# --- Configuration ---
found_info = False


ip = "scanme.nmap.org"
port = int(input("Enter port number: "))

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(1.0)
sock.connect((ip, port))

msg = b"HEAD / HTTP/1.1\r\nHost: scanme.nmap.org\r\n\r\n"

try:
    banner = sock.recv(1024)
except socket.timeout:
    sock.sendall(msg)
    banner = sock.recv(1024)
sock.close()

banner_str = banner.decode().strip()
lines = banner_str.splitlines()

for line in lines:
    if line.startswith("Server:"):
        found_info = True
        print(f"Server Banner: {line}")
        
        software = line.replace("Server: ", "").strip()
        print(f"Target Software: {software}")

if not found_info:
    print(banner_str)