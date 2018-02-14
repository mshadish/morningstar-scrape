#!/usr/bin/env python
import sys
import smtplib

NUMBER = '9254871921@txt.att.net'


if __name__ == '__main__':
    # start the server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()

    # log in
    gmail_pass = None
    with open('/users/mshadish/cef_model/.gmail_pass', 'r') as infile:
        gmail_pass = infile.read()

    server.login('mshadish@gmail.com', gmail_pass)

    # read in the stdin input
    all_input = sys.stdin.readlines()
    message_str = ''.join(all_input)
    
    # send the message
    server.sendmail('mshadish@gmail.com', NUMBER, message_str)

    # close the server
    server.close()
