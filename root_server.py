'''
Name: Carlos Alvarenga
Student id: 5197501
Email: alvar357@umn.edu
Filename: root_server.py
Description: Program encompasses the functionality of the root DNS server
             programs that either redirects the default local DNS server to the
             appropriate DNS server or directly contacts said DNS server to
             receive the response and send it back to the default local DNS server.
'''

import sys
import socket

domains = {} # structure that contains domain ip-port information

def server_shutdown(sock):
    '''
    Function triggered by user's ctrl-c keyboard interrupt signaling server
    shutdown.Broadcast messages 'shutdown' are sent to all other servers
    notifying them of shutdown and socket connections are closed.
    '''
    print('\nCommencing root DNS server shutdown')

    # Send broadcast message to default local DNS server
    s = socket.socket()
    s.connect(('127.0.0.1', 5352))
    s.send("shutdown")
    s.close()

    # Send broadcast messages to remaining DNS servers
    for item in domains:
        s = socket.socket()
        ip = domains.get(item)[0]
        port = domains.get(item)[1]
        s.connect((ip, port))
        s.send("shutdown")
        s.close()
    print('Root DNS server socket closed')
    sock.close()

def map_domains(filename):
    '''
    Function that maps .com, .org, .gov domains to appropriate port and ip
    numbers based on server.dat file and stores this information in appropriate
    data structure.
    '''
    file = open(filename, "r");
    try:
        for line in file:
            line = line.strip("\n").strip("\r")
            line = line.split(" ")
            domains[line[0]] = [line[1].lower(), int(line[2])]
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

def resolve_query(client_msg, server_id):
    '''
    Function that determines whether the client request has to be sent directly
    to the .com, .org or .gov server or it should redirect the default local DNS
    server to one of the three mentioned server. The appropriate response
    message string for either case is returned.
    '''
    client_msg_arr = client_msg.split(", ")
    hostname = client_msg_arr[1].split(".")
    domain = hostname[len(hostname)-1].lower()
    dns_server_ip = domains.get(domain)[0]
    dns_server_port = domains.get(domain)[1]
    if (client_msg_arr[2].lower() == 'i'): # iterative request
        return ('0x01, ' + server_id + ', ' + dns_server_ip + ', ' + str(dns_server_port))
    else: # recursive request
        s = socket.socket()
        s.connect((dns_server_ip, dns_server_port))
        print('Connected to DNS server ' + domain)
        s.send(client_msg.encode('utf-8'))
        print('Message sent to DNS server: ' + client_msg)
        response = s.recv(1024).decode('utf-8')
        print('Response received from DNS server: ' + response)
        s.close()
        print('DNS server socket closed')
        return format_message(False, response, server_id)

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
        print('Message recieved from default local DNS server: ' + client_msg)
        client_msg = format_message(True, client_msg, server_id)
        response = resolve_query(client_msg, server_id)
        clientsocket.send(response.encode('utf-8'))
        print('Response sent to default local DNS server: ' + response)
    clientsocket.close()
    print('Default local DNS socket closed\n')
    return shutdown

def server(server_id, server_port, mapping_file, servers_list):
    '''
    Main function where root DNS server connection is set up to recieve and send
    messages and is closed when appropriate.
    '''
    s = socket.socket()                     # Create a socket object
    ip = '127.0.0.1'
    shutdown = False                        # Track any existing server shutdown

    try:
        s.bind((ip, int(server_port)))      # Bind to the port
        s.listen(5)                         # Now wait for client connection.
        print('Root DNS Server started!')
        print('Waiting for clients...')
        while True and not shutdown:
           c, addr = s.accept()             # Establish connection with client.
           print ('Connect to default local DNS server' + str(addr))
           shutdown = talk_with_server(c, addr, server_id, shutdown)
        s.close()
        print('Root DNS server socket closed')
    except KeyboardInterrupt:
        server_shutdown(s)

if __name__ == '__main__':
    map_domains(sys.argv[4])
    server(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
