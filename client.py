'''
Name: Carlos Alvarenga
Student id: 5197501
Email: alvar357@umn.edu
Filename: client.py
Description: Program encompasses the functionality of the client program that
             receives user input requests to resolve.
'''

import sys
import socket

has_written = False # keeps track if something has been written to the log file

def write_to_file(filename, input, output):
    '''
    Function that writes client request message and server response message
    to the appropriate client log file.
    '''
    if (input != 'q') and (output != 'closed'):
        f = open(filename,'a')
        global has_written
        if has_written:
            f.write('\n\n' + input + '\n')
        else:
            f.write(input + '\n')
            has_written = True
        f.write(output)
        f.close();

def clean_message(msg):
    '''
    Function that returns message with removed leading and trailing whitespaces
    from the client id, hostname and request fields.
    '''
    cleaned_msg = ''
    msg_arr = msg.split(",")
    for i in range(len(msg_arr)):
        cleaned_msg += msg_arr[i].strip()
        if (i != len(msg_arr) - 1):
            cleaned_msg += ', '
    return cleaned_msg

def client(client_id, server_ip, server_port):
    '''
    Main function where user requests are accepted and resolved by contacting
    the default local DNS server.
    '''
    filename = client_id + '.log'
    f = open(filename, 'w'); # create/overwrite client log file
    f.close()

    s = socket.socket()
    s.connect((server_ip, int(server_port)))
    print('Client started!')
    response = '' # response from default local DNS server
    while response != 'shutdown':  # default local DNS server has been shut down or user indicated termination of client program
        msg = raw_input('Enter a message request: ')
        if (msg != 'q'):
            cleaned_msg = clean_message(msg)
            s.send(cleaned_msg.encode('utf-8'))
            response = s.recv(1024).decode('utf-8')
            if (response != 'shutdown'):
                write_to_file(filename, msg, response)
            print('Response received from default DNS server: ' + response + '\n')
        else:
            response = 'shutdown'
    s.close()
    print('Client socket closed')

if __name__ == '__main__':
    client(sys.argv[1], sys.argv[2], sys.argv[3])
