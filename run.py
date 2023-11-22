import socket
import threading
import random
import logging
import struct
from socks5 import Socks5Server, Socks5Client, DataExchanger
from authservice import AuthService
import select

# Configurazione del logging
logging.basicConfig(filename='geotcprelay.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', filemode='w')

class GeoTcpRelay:
    def __init__(self):
        self.producers = []
        self.client_producer_mappings = {}  # Mappatura tra dispositivi A e B
        self.lock = threading.Lock()

    def start_server(self, host, port_b, port_a):
        threading.Thread(target=self.listen_on_port, args=(host, port_b, True)).start()  # Ascolta i dispositivi B
        threading.Thread(target=self.listen_on_port, args=(host, port_a, False)).start()  # Ascolta i dispositivi A
        logging.info("Server started and listening on ports %d and %d", port_b, port_a)

    def listen_on_port(self, host, port, is_device_b):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(5)
        logging.info("Listening for %s on port %d", 'Producer' if is_device_b else 'Client', port)

        while True:
            client_sock, addr = server_socket.accept()
            logging.info("Accepted connection from %s:%d", *addr)
            if is_device_b:
                threading.Thread(target=self.handle_producer, args=(client_sock,)).start()
            else:
                threading.Thread(target=self.handle_client, args=(client_sock,)).start()
        
    def unregister_producer(self, producer_socket, close_socket=False):
        with self.lock:
            if producer_socket in self.producers:
                self.producers.remove(producer_socket)
                logging.info("Producer with socket %s unregistered", producer_socket)
        
        if close_socket:
            producer_socket.close()
            logging.info("Connection closed with Producer with socket: %s", producer_socket)

    def unregister_client(self, client_socket,close_socket=False):
        with self.lock:
            if client_socket in self.client_producer_mappings:
                del self.client_producer_mappings[client_socket]
                logging.info("Client with socket %s unregistered", client_socket)
        
        if close_socket:
            client_socket.close()
            logging.info("Connection closed with Client with socket: %s", client_socket)


    def handle_producer(self, producer_socket):

        try:

            packed_data = producer_socket.recv(128)  # Ricevi il messaggio di handshake
            api_key_length = struct.unpack('!I', packed_data[:4])[0]
            api_key = struct.unpack(f"!{api_key_length}s", packed_data[4:])[0].decode('utf-8')


            status = AuthService().login_producer(api_key)
            if not status:
                packet = struct.pack("!B", 0)  # Invia un messaggio di errore al dispositivo B
                producer_socket.sendall(packet)
                raise Exception("Invalid authentication handshake")
            
            packet = struct.pack("!B", 1)
            producer_socket.sendall(packet)

            with self.lock:
                self.producers.append(producer_socket)
                logging.info("Producer connected with socket: %s", producer_socket)
        
        except Exception as e:
            logging.error("Error during handshake with Producer: %s", e)
            logging.warning(e)
            self.unregister_producer(producer_socket, close_socket=True)
            return

    def handle_client(self, client_socket):

        try:

            with self.lock:
                selected_producer = self.select_producer_for_client()
                if selected_producer:
                    self.client_producer_mappings[client_socket] = selected_producer
                    logging.info("Client connected and mapped to Producer with socket: %s", selected_producer)
                else:
                    raise Exception("No Producer available")
        
        except Exception as e:
            logging.warning("Closing connection to Client with socket: %s", client_socket)
            logging.warning(e)
            self.unregister_client(client_socket,close_socket=True)
            return

        self.exchange_data(client_socket, self.client_producer_mappings[client_socket])

    def select_producer_for_client(self):
        random_producer = random.choice(self.producers) if len(self.producers) > 0 else None
        #remove producer from list
        if random_producer:
            self.producers.remove(random_producer)
        return random_producer

    def exchange_data(self, client_socket, producer_socket):
        try:
            DataExchanger(client_socket, producer_socket).exchange_data()
        except Exception as e:
            logging.warning("Closing connection to Client with socket: %s", client_socket)
            logging.warning(e)
        finally:

            self.unregister_client(client_socket, close_socket=True)
            self.unregister_producer(producer_socket, close_socket=True)

            
            

if __name__ == "__main__":
    server = GeoTcpRelay()
    HOST = "0.0.0.0"
    PORT_B = 30000  # Porta per i dispositivi B
    PORT_A = 60000  # Porta per i dispositivi A
    server.start_server(HOST, PORT_B, PORT_A)