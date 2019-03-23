'''
Name: Carlos Alvarenga
Student id: 5197501
Email: alvar357@umn.edu
Filename: default_server.py
Description: Program encompasses the functionality of the default local DNS server
             program that forwards client requests to other servers and sends back
             the response to the client.
'''

import sys
import socket
import thread

cached_mappings = {} # structure that contains succesful responses to past client requests
domains = {} # structure that contains domain ip-port information
clients = set() # structure that contains active client sockets
log_has_been_written = False # keeps track if something has been written to the server log file
mapping_has_been_written = False # keeps track if something has been written to the mapping log file
has_been_closed = False # keeps track of whether or not any server has been shut down

def server_shutdown(sock):
    '''
    Function triggered by user's ctrl-c keyboard interrupt signaling server shutdown.
    Broadcast messages 'shutdown' are sent to all other servers notifying them of
    shutdown and socket connections are closed.
    '''
    print('\nCommencing default local server shutdown')

    # sent broadcast message to connected clients and close corresponding sockets
    for c in clients:
        c.send('shutdown')
        c.close()

    if not has_been_closed: # if the other servers haven't been shut down already
        # Send broadcast message to root DNS server
        s = socket.socket()
        s.connect(('127.0.0.1', 5353))
        s.send("shutdown")
        s.close()

        # Send broadcast messages to remaining DNS servers
        for port in domains:
            s = socket.socket()
            ip = domains.get(port)
            s.connect((ip, int(port)))
            s.send("shutdown")
            s.close()
    sock.close()
    print('Default local DNS server socket closed')

def write_to_file(filename, content, is_client_msg):
    '''
    Function that writes either
    1) message sent/recieved from server OR
    2) resolved query mapping
    to the server log file or mapping log file, respectively.
    '''
    f = open(filename,'a')
    if filename == 'mapping.log':
        global mapping_has_been_written
        if mapping_has_been_written:
            f.write('\n' + content)
        else:
            f.write(content)
            mapping_has_been_written = True
    else:
        global log_has_been_written
        if log_has_been_written:
            if is_client_msg: # check if the message being written is from the client
                f.write('\n\n' + content)
            else:
                f.write('\n' + content)
        else:
            f.write(content)
            log_has_been_written = True
    f.close();

def map_domains(filename):
    '''
    Function that maps .com, .org, .gov domains to appropriate port and ip numbers
    based on server.dat file and stores this information in appropriate data structure.
    '''
    file = open(filename, "r");
    try:
        for line in file:
            line = line.strip("\n").strip("\r")
            line = line.split(" ")
            domains[line[2]] = line[1]
    finally:
        file.close()

def cache_mapping(client_msg, response):
    '''
    Function that stores information about resolved query to the appropriate
    data structure and write it to the mapping log file.
    '''
    client_msg_arr = client_msg.split(", ")
    response_arr = response.split(", ")
    hostname = client_msg_arr[1].lower()
    response_code = response_arr[0]
    ip = response_arr[2]
    if (hostname not in cached_mappings) and (response_code != '0xFF'):
        # make sure hostname can be resolved and hasn't been cached already
        cached_mappings[hostname] = [ip, response]
        file_cached_mapping = hostname + " " + ip
        write_to_file('mapping.log', file_cached_mapping, False)

def get_cached_mapping(client_msg):
    '''
    Function that returns stored response to resolve a query/hostname if it has
    already been resolved.
    '''
    client_msg_arr = client_msg.split(", ")
    hostname = client_msg_arr[1].lower()
    if (hostname in cached_mappings):
        return cached_mappings.get(hostname)[1]
    return None

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

def resolve_query(client_msg, root_msg, server_id, filename):
    '''
    Function that determines whether the response from the root server has to be
    sent to the client (recursive request) or other DNS server that can resolve
    the request (iterative request) and get back the response from this corresponding
    server. The appropriate response message string for either case is returned.
    '''
    client_msg_arr = client_msg.split(", ")
    request = client_msg_arr[2].lower()
    if (request == "i"): # iterative
        s = socket.socket()
        root_msg_arr = root_msg.split(", ")
        ip = root_msg_arr[2]
        port = int(root_msg_arr[3])
        s.connect((ip, port))
        print('Connected to DNS server port ' + str(port))
        write_to_file(filename, client_msg, False)
        print('Message sent to DNS server: ' + client_msg)
        s.send(client_msg.encode('utf-8'))
        response = s.recv(1024).decode('utf-8')
        print('Response received from DNS server: ' + response)
        write_to_file(filename, response, False)
        s.close()
        print('DNS server socket closed')
        return response
    return root_msg

def talk_with_server(client_msg, server_id, filename):
    '''
    Function responsible for talking with root server to determine the next steps
    towards resolving the client request. The correct response message to the
    client message is returned.
    '''
    s = socket.socket()
    s.connect(('127.0.0.1', 5353))
    print('Connected to root DNS server')
    write_to_file(filename, client_msg, False)
    s.send(client_msg.encode('utf-8'))
    print('Message sent to root DNS server: ' + client_msg)
    root_msg = s.recv(1024).decode('utf-8')
    print('Response received from root DNS server: ' + root_msg)
    write_to_file(filename, root_msg, False)
    s.close()
    print('Root DNS server socket closed')
    return resolve_query(client_msg, root_msg, server_id, filename)

def invalid_message(client_msg):
    '''
    Message that return True or False depending on whether or not the client request
    is invalid.
    '''
    client_msg_arr = client_msg.split(", ")
    if len(client_msg_arr) != 3:
        return True
    else:
        request = client_msg_arr[2]
        domain = client_msg_arr[1].split(".")
        domain = domain[len(domain) - 1]
        if (request.lower() not in ['i','r']) or (domain.lower() not in ['com','gov','org']):
            return True
        return False

def new_client(clientsocket, addr, server_id, filename):
    '''
    Function that talks with the client to recieve requests and respond with the
    correct response message
    '''
    global has_been_closed
    while True:
        response = ''
        client_msg = clientsocket.recv(1024).decode('utf-8')
        if not client_msg:
            break
        if (client_msg == 'shutdown'): # recieve broadcast message from another server
            has_been_closed = True
            break
        print('Message recieved from client: ' + client_msg)
        write_to_file(filename, client_msg, True)
        has_been_closed = False
        if (invalid_message(client_msg)):
            response = '0xEE, ' + server_id + ', Invalid format'
        else:
            response = get_cached_mapping(client_msg);
            if not response:
                client_msg = format_message(True, client_msg, server_id)
                response = talk_with_server(client_msg, server_id, filename)
                response = format_message(False, response, server_id)
                cache_mapping(client_msg, response)
        write_to_file(filename, response, False)
        clientsocket.send(response.encode('utf-8'))
        print('Response sent to client: ' + response + '\n')
    clients.remove(clientsocket)
    clientsocket.close()
    print('Client socket closed')

def server(server_id, server_port, mapping_file, servers_list):
    '''
    Main function where default local DNS server connection is set up to recieve
    and send messages and is closed when appropriate.
    '''
    filename = server_id + '.log'
    f = open(filename, 'w');        # create/overwrite server log file
    f.close()
    f = open('mapping.log', 'w')    # create/overwrite mapping log file
    f.close()
    s = socket.socket()                        # Create a socket object
    ip = '127.0.0.1'
    port = int(server_port)

    try:
        s.bind((ip, int(server_port)))       # Bind to the port
        s.listen(5)                          # Now wait for client connection.
        print ('Default local DNS Server started!')
        print ('Waiting for clients...')
        while True:
           c, addr = s.accept()              # Establish connection with client.
           clients.add(c)
           print ('Connected to client ' + str(addr))
           # spawn new thread for new client
           thread.start_new_thread(new_client,(c, addr, server_id, filename))
    except KeyboardInterrupt: # user has manually indicated server shutdown
        server_shutdown(s)

if __name__ == '__main__':
    map_domains(sys.argv[4])
    server(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
