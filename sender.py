"""
    Sample code for Sender (multi-threading)
    Python 3
    Usage: python3 sender.py receiver_port sender_port FileToSend.txt max_recv_win rto
    coding: utf-8

    Notes:
        Try to run the server first with the command:
            python3 receiver_template.py 9000 10000 FileReceived.txt 1 1
        Then run the sender:
            python3 sender_template.py 11000 9000 FileToReceived.txt 1000 1

    Author: Rui Li (Tutor for COMP3331/9331)
"""
# here are the libs you may find it useful:
import datetime, time
import pickle
import random  # to calculate the time delta of packet transmission
import logging, sys, os  # to write the log
import socket  # Core lib, to send packet via UDP socket
from threading import Lock, Thread  # (Optional)threading will make the timer easily implemented
from enum import Enum
from collections import deque
# larger buffer size to account for size of class
BUFFERSIZE = 2048

# data types for segment header 
class Type(Enum):
    DATA = 0
    ACK = 1
    SYN = 2
    FIN = 3
    RESET = 4

# state variables 
class State(Enum):
    CLOSED = "CLOSED"
    SYN_SENT = "SYN_SENT"
    EST = "EST"
    CLOSING = "CLOSING"
    LISTEN = "LISTEN"
    FIN_WAIT = "FIN_WAIT"
    TIME_WAIT = "TIME_WAIT"

# Segment class to represent STP segment
class Segment:
    def __init__(self, seq_no, packet_type, data, sender_time) -> None:
        self.packet_type = packet_type.to_bytes(2, "big")
        self.seq_no = seq_no.to_bytes(2,"big")
        self.data = data
        self.sender_time_sent = sender_time
        self.syn_ack = False 
        self.fin_ack = False
        self.retransmitted = 0

    def get_seq_int(self):
        return int.from_bytes(self.seq_no, byteorder='big')

    def get_packet_int(self):
        return int.from_bytes(self.packet_type, byteorder='big')
    
    def set_fin_ack(self, value):
        self.fin_ack = value

    def set_syn_ack(self, value):
        self.syn_ack = value

    def add_retransmission(self):
        self.retransmitted += 1

class Sender:
    def __init__(self, sender_port: int, receiver_port: int, filename: str, max_win: int, rot: int) -> None:
        '''
        The Sender will be able to connect the Receiver via UDP
        :param sender_port: the UDP port number to be used by the sender to send PTP segments to the receiver
        :param receiver_port: the UDP port number on which receiver is expecting to receive PTP segments from the sender
        :param filename: the name of the text file that must be transferred from sender to receiver using your reliable transport protocol.
        :param max_win: the maximum window size in bytes for the sender window.
        :param rot: the value of the retransmission timer in milliseconds. This should be an unsigned integer.
        '''
        # intialising deque to simulate FIFO
        self.window = deque()
        self.current_seq_no = random.randint(1, pow(2,16)-1)
        self.state = State.CLOSED.value
        self.filename = filename
        self.sender_port = int(sender_port)
        self.receiver_port = int(receiver_port)
        self.sender_address = ("127.0.0.1", self.sender_port)
        self.receiver_address = ("127.0.0.1", self.receiver_port)
        self.max_win = int(max_win) 
        self.rto = float(rot)
        self.receiver_log = open("Sender_log.txt", "w")
        # boolean to distinguish whether all file contents have been sent
        self.file_transferred = False

        # init the UDP socket
        logging.debug(f"The sender is using the address {self.sender_address}")
        self.sender_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.sender_socket.bind(self.sender_address)

        self._is_active = True  # for the multi-threading
        listen_thread = Thread(target=self.listen)
        listen_thread.start()

        # lock will be used in various places to ensure the sender window is not 
        # shared between threads
        self.lock = Lock()
        # todo add codes here
        pass


    '''
    this function will be called to initiate a connection with receiver
    '''
    def ptp_open(self):
        segment = Segment(self.current_seq_no, Type.SYN.value, None, time.time())
        # start timer
        self.start_time = time.time()
        self.sender_socket.sendto(pickle.dumps(segment), self.receiver_address)
        self.log(segment)
        self.increase_seq_no(1)
        self.window.append(segment)
        self.state = State.SYN_SENT.value

        # begin retransmission thread
        self._is_retransmitting = True
        retransmit_thread = Thread(target=self.retransmit)
        retransmit_thread.start()
        self.ptp_send()
        pass

    '''
    this function contains file sending functionality
    '''
    def ptp_send(self):
        with open(self.filename, "rb") as file:
            i = 0
            while True: 
                # only run if state is EST and the upcoming file.read() plus window size is
                # less than the maximum window. 
                if (self.get_window_size() + self.peek_content(file)) <= self.max_win and self.state == State.EST.value:
                    content = file.read(1000)
                    if content:
                        self.lock.acquire()
                        segment = Segment(self.current_seq_no, Type.DATA.value, content, time.time())
                        self.window.append(segment)
                        self.sender_socket.sendto(pickle.dumps(segment), self.receiver_address)
                        self.log(segment)
                        self.increase_seq_no(len(content))
                        self.lock.release()
                    else:
                        self.state = State.CLOSING.value
                        self.file_transferred = True
                        self.fin_sent = False
                        break

    def ptp_close(self):
        self.state = State.CLOSED.value
        self._is_active = False  
        self._is_retransmitting = False
        self.fin_sent = True
        self.sender_socket.close()
        pass

    def retransmit(self):
        while self._is_retransmitting:
            self.lock.acquire() 
            # check if window has contents
            if (len(self.window) > 0):
                # check if timer for first unacked seg is equal to/greater than rto
                # or received triple duplicate acks 
                if (time.time() - self.window[0].sender_time_sent) >= self.rto or self.triple_ack is True:
                    # check if syn or fin packet has been retransmitted three times 
                    if (self.window[0].retransmitted == 3 and 
                            (self.window[0].get_packet_int() == Type.SYN.value or 
                            self.window[0].get_packet_int() == Type.FIN.value)):
                        segment = Segment(0, Type.RESET.value, None, time.time())
                        self.sender_socket.sendto(pickle.dumps(segment), self.receiver_address)
                        self.log(segment)
                        self.ptp_close()
                        os._exit(1)

                    # retransmit oldest unacked segment
                    logging.debug(f"packet {self.window[0].get_seq_int()} has been retransmitted")
                    self.window[0].sender_time_sent = time.time()
                    self.window[0].add_retransmission()
                    self.sender_socket.sendto(pickle.dumps(self.window[0]), self.receiver_address)
                    self.log(self.window[0])
                    self.triple_ack = False 
            # if window doesn't have content and file has finished transmitting data
            else:
                if (self.file_transferred is True and self.fin_sent is False):
                    self.state = State.FIN_WAIT.value
                    segment = Segment(self.current_seq_no, Type.FIN.value, None, time.time())
                    self.window.append(segment)
                    self.sender_socket.sendto(pickle.dumps(segment), self.receiver_address)
                    self.log(segment)
                    self.increase_seq_no(1)
            self.lock.release()

    def listen(self):
        '''(Multithread is used)listen the response from receiver'''
        logging.debug("Sub-thread for listening is running")
        # deque to store acks coming in, limited to a size of three 
        # if all acks are equal, triple duplicate ack detected 
        ack_deque = deque()
        self.triple_ack = False
        while self._is_active:
            incoming_message, _ = self.sender_socket.recvfrom(BUFFERSIZE)
            # deserialize message 
            segment = pickle.loads(incoming_message)
            self.lock.acquire()
            self.log(segment)
            self.lock.release()
            # receiving SYN ACK
            if segment.get_packet_int() == Type.ACK.value and segment.syn_ack is True:
                self.lock.acquire() 
                self.window.popleft()
                self.lock.release()
                self.state = State.EST.value

            # receiving FIN ACK
            elif segment.get_packet_int() == Type.ACK.value and segment.fin_ack is True:
                self.lock.acquire() 
                self.window.popleft()
                self.state = State.CLOSED.value
                self.ptp_close()
                self.lock.release()
                exit()
                
            # receiving DATA ACK
            elif (segment.get_packet_int() == Type.ACK.value and 
                    (segment.fin_ack is False and segment.syn_ack is False)):
                # append acks as they come in. size limited to 3
                if (len(ack_deque) == 3):
                    ack_deque.popleft()
                ack_deque.append(segment.seq_no)
                # check for triple duplicate ack
                if (len(ack_deque) == 3):
                    if (ack_deque[0] == ack_deque[1] and ack_deque[0] == ack_deque[2]):
                        self.triple_ack = True

                index = 0
                ack_found = False 
                self.lock.acquire()
                for s in list(self.window):
                    # if ack received is acknowledging a segment within window 
                    if (segment.get_seq_int() == (s.get_seq_int()+len(s.data)) % (pow(2,16)-1)):
                            index += 1
                            ack_found = True
                            break
                    index += 1
                self.lock.release()
                # error checking: window should never be empty upon receiving an ack
                # except for during premature retransmission, however this was not implemented 
                if (index == 0 and ack_found == False):
                    segment = Segment(0, Type.RESET.value, None, time.time())
                    self.sender_socket.sendto(pickle.dumps(segment), self.receiver_address)
                    self.log(segment)
                    self.ptp_close()
                    os._exit(1)


                # if no 
                if (ack_found == False):
                    continue

                # cumulative acks: remove all segments in the window up to segment acked
                self.lock.acquire()
                for i in range(index):
                    self.window.popleft()
                self.lock.release()    

            elif segment.get_packet_int() == Type.RESET.value:
                os._exit(1)
            

    def run(self):
        '''
        This function contain the main logic of the receiver
        '''
        # todo add/modify codes here
        self.ptp_open()

    # HELPERS

    def increase_seq_no(self, bytes):
        self.current_seq_no += bytes
        if self.current_seq_no > (pow(2,16)-1):
            self.current_seq_no = self.current_seq_no%(pow(2,16)-1)

    def get_window_size(self):
        size = 0
        for segment in self.window:
            if segment.data != None:
                size += len(segment.data)
        return size

    def peek_content(self, file):
        pos = file.tell()
        content = file.read(1000)
        file.seek(pos)
        return len(content)
    
    def log(self, segment):
        if segment.get_packet_int() == Type.DATA.value:
            logging.warning(f"snd\t{round((time.time()-self.start_time)*1000, 2)}\tDATA\t{segment.get_seq_int()}\t{len(segment.data)}")
        elif segment.get_packet_int() == Type.ACK.value:
            logging.warning(f"rcv\t{round((time.time()-self.start_time)*1000, 2)}\tACK\t{segment.get_seq_int()}\t0")
        elif segment.get_packet_int() == Type.SYN.value:
            logging.warning(f"snd\t0\tSYN\t{segment.get_seq_int()}\t0")
        elif segment.get_packet_int() == Type.FIN.value:
            logging.warning(f"snd\t{round((time.time()-self.start_time)*1000, 2)}\tFIN\t{segment.get_seq_int()}\t0")
        else:
            print("RESET has been sent/received... Program exiting now")

if __name__ == '__main__':
    # logging is useful for the log part: https://docs.python.org/3/library/logging.html
    logging.basicConfig(
        filename="Sender_log.txt",
        # stream=sys.stderr,
        level=logging.WARNING,
        format='%(message)s',
        datefmt='%Y-%m-%d:%H:%M:%S')

    if len(sys.argv) != 6:
        print(
            "\n===== Error usage, python3 sender.py sender_port receiver_port FileReceived.txt max_win rot ======\n")
        exit(0)

    sender = Sender(*sys.argv[1:])
    sender.run()

