import socket
import time

HOST = "localhost"
PORT = 9999

print("===== LOG PRODUCER STARTED =====")
print(f"Waiting for Spark on {HOST}:{PORT}...")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:

    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server.bind((HOST, PORT))
    server.listen(1)

    conn, addr = server.accept()

    print(f"Spark connected from {addr}")
    print("Sending log data...\n")

    with conn:
        with open("data/application.log", "r") as file:

            for line in file:
                line = line.strip()

                if line:
                    try:
                        conn.sendall(
                            (line + "\n").encode()
                        )

                        print(f"Sent: {line}")

                        time.sleep(1)

                    except BrokenPipeError:
                        print("\nSpark closed the connection.")
                        break

    print("\n===== ALL LOGS SENT =====")
