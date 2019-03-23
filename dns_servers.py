'''
Name: Carlos Alvarenga
Student id: 5197501
Email: alvar357@umn.edu
Filename: dns_servers.py
Description: Program encompasses the functionality of the .com, .org, .gov DNS server
             programs that resolve DNS queries by referencing the appropriate
             mapping file.
'''

import sys
import socket

mappings = {} # strcuture that contains hostname mappings according to .dat file
domains = {} # structure that contains domain ip-port information

def server_shutdown(sock, server_port):
    '''
    Function triggered by user's ctrl-c keyboard interrupt signaling server
    shutdown. Broadcast messages 'shutdown' are sent to all other servers
    notifying them of shutdown and socket connections are closed.
    '''
    print('\nCommencing DNS server shutdown...')

    # Send broadcast message to default local DNS server
    s = socket.socket()
    s.connect(('127.0.0.1', 5352))
    s.send("shutdown")
    s.close()

    # Send broadcast message to root DNS server
    s = socket.socket()
    s.connect(('127.0.0.1', 5353))
    s.send("shutdown")
    s.close()

    # Send broadcast messages to remaining DNS servers
    for domain_port in domains:
        if domain_port != server_port:
            s = socket.socket()
            ip = domains.get(domain_port)
            port = int(domain_port)
            s.connect((ip, port))
            s.send("shutdown")
            s.close()
    sock.close()
    print('DNS server socket closed')

def preprocess_server(filename):
    '''
    Function that reads hostname mappings from .dat file to store them in
    appropriate data structure so they can be used to resolve queries.
    '''
    file = open(filename, "r");
    try:
        for line in file:
            line = line.strip("\n").strip("\r")
            line = line.split(" ")
            hostname = format_hostname(line[0])
            mappings[hostname.lower()] = line[1]
    finally:
        file.close()

def map_domains(filename):
    '''
    Function that maps .com, .org, .gov domains to appropriate port and ip
    addresses based on server.dat file and stores this information in
    appropriate data structure.
    '''
    file = open(filename, "r");
    try:
        for line in file:
            line = line.strip("\n").strip("\r")
            line = line.split(" ")
            domains[line[2]] = line[1]
    finally:
        file.close()

def format_message(is_received, msg, server_id):
    '''
    Function that formats client message to replace id field with the id of the
    current DNS server to adhere to the project write-up template and send the
    message back to another server. The reformatted message is returned.
    '''
    msg_arr = msg.split(", ")
    if is_received: # message is request from client
        return (server_id + ', ' + msg_arr[1] + ', ' + msg_arr[2])
    else:
        return (msg_arr[0] + ', ' + server_id + ', ' + msg_arr[2])

def format_hostname(hostname):
    '''
    Function that removes 'www' from original hostname and sets the name to
    lowercase so that it be searched for and compared to see if it exists in
    DNS mapping. This reformatted hostname is then returned.
    '''
    hostname = hostname.split(".")
    hostname_str = ''
    index = 0;
    for i in hostname:
        if (index != 0) or (i.lower() != 'www'):
            hostname_str += i
            if (index != len(hostname)-1):
                hostname_str += '.'
        index +=  1
    return hostname_str.lower()

def resolve_query(client_msg, server_id):
    '''
    Function that determines whether a mapping for queried hostname exists in
    DNS mapping and returns the correct response string.
    '''
    client_msg_arr = client_msg.split(", ")
    hostname = format_hostname(client_msg_arr[1])
    if (hostname not in mappings):
        return ('0xFF, ' +  server_id + ', Host not found')
    else:
        return ('0x00, ' + server_id + ', ' + mappings.get(hostname))

def talk_with_server(clientsocket, addr, server_id, shutdown):
    '''
    Function responsible for talking with DNS servers by accepting requests and
    sending out the correct responses. The server shutdown status after
    communication is returned.
    '''
    while True:
        client_msg = clientsocket.recv(1024).decode('utf-8')
        if not client_msg:
            break
        if client_msg == 'shutdown': # recieve broadcast message from another server
            shutdown = True
            break
        print('Message recieved from server: ' + client_msg)
        client_msg = format_message(True, client_msg, server_id)
        response = resolve_query(client_msg, server_id)
        clientsocket.send(response.encode('utf-8'))
        print('Response sent to server: ' + response)
    clientsocket.close()
    print('Server socket closed\n')
    return shutdown

def server(server_id, server_port, mapping_file, servers_list):
    '''
    Main function where DNS server connection is set up to recieve and send
    messages and is closed when appropriate.
    '''
    s = socket.socket()                     # Create a socket object
    ip = domains.get(server_port)
    shutdown = False                        # Track any existing server shutdown

    try:
        s.bind((ip, int(server_port)))      # Bind to the port
        s.listen(5)                         # Now wait for client connection.
        print ('DNS Server started!')
        print ('Waiting for clients...')
        while True and not shutdown:
           c, addr = s.accept()             # Establish connection with client.
           print('Connected to server ' + str(addr))
           shutdown = talk_with_server(c, addr, server_id, shutdown)
        s.close()
        print('DNS server socket closed')
    except KeyboardInterrupt: # user has manually indicated server shutdown
        server_shutdown(s, server_port)

if __name__ == '__main__':
    preprocess_server(sys.argv[3])
    map_domains(sys.argv[4])
    server(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
