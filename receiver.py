"""
    Sample code for Receiver
    Python 3
    Usage: python3 receiver.py receiver_port sender_port FileReceived.txt flp rlp
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
import pickle  # to calculate the time delta of packet transmission
import logging, sys, os  # to write the log
import socket  # Core lib, to send packet via UDP socket
from threading import Thread  # (Optional)threading will make the timer easily implemented
import random  # for flp and rlp function
from sender import Type, State, Segment

# larger buffer size to account for size of class
BUFFERSIZE = 2048

class Receiver:
    def __init__(self, receiver_port: int, sender_port: int, filename: str, flp: float, rlp: float) -> None:
        '''
        The server will be able to receive the file from the sender via UDP
        :param receiver_port: the UDP port number to be used by the receiver to receive PTP segments from the sender.
        :param sender_port: the UDP port number to be used by the sender to send PTP segments to the receiver.
        :param filename: the name of the text file into which the text sent by the sender should be stored
        :param flp: forward loss probability, which is the probability that any segment in the forward direction (Data, FIN, SYN) is lost.
        :param rlp: reverse loss probability, which is the probability of a segment in the reverse direction (i.e., ACKs) being lost.
        '''

        # using list to store buffer of segments
        self.buffer = []

        self.current_seq_no = 0
        self.state = State.CLOSED.value

        self.address = "127.0.0.1"  # change it to 0.0.0.0 or public ipv4 address if want to test it between different computers
        self.receiver_port = int(receiver_port)
        self.sender_port = int(sender_port)
        self.server_address = (self.address, self.receiver_port)
        self.flp = flp
        self.rlp = rlp
        self.start_time = 0

        # opening text file 
        self.f = open(filename, "w")
        self.receiver_log = open("Receiver_log.txt", "w")

        # init the UDP socket
        # define socket for the server side and bind address
        logging.debug(f"The receiver is using the address {self.server_address} to receive message!")
        self.receiver_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.receiver_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.receiver_socket.bind(self.server_address)
        self.state = State.LISTEN.value
        pass


    def run(self) -> None:
        '''
        This function contains the main logic of the receiver
        '''
        while True:
            # try to receive any incoming message from the sender
            incoming_message, sender_address = self.receiver_socket.recvfrom(BUFFERSIZE)
            sender_segment = pickle.loads(incoming_message)

            # if packet is lost from sender->receiver.
            # reset packets cannot be lost 
            if self.is_flp() and sender_segment.get_packet_int() != Type.RESET.value:
                self.log(sender_segment, True)
                continue

            self.log(sender_segment, False)
            # initialising receiver segment
            receiver_segment = None
            
            if sender_segment.get_packet_int() == Type.DATA.value:
                

                # packet is in order
                if (self.current_seq_no == sender_segment.get_seq_int()):
                    self.f.write(sender_segment.data.decode(encoding='UTF-8'))
                    self.increase_seq_no(len(sender_segment.data))
                    # check if any buffered contents can now be written to file
                    self.check_buffer_contents(sender_segment)
                # if packet is out of order
                else:
                    self.buffer.append(sender_segment)
                # check if packet is sent during wrong state
                if (self.state == State.LISTEN.value or self.state == State.TIME_WAIT.value):
                    logging.warning("data state reset")
                    receiver_segment = Segment(self.current_seq_no, Type.RESET.value, None, sender_segment.sender_time_sent)
                else:
                    receiver_segment = Segment(self.current_seq_no, Type.ACK.value, None, sender_segment.sender_time_sent)

            elif (sender_segment.get_packet_int() == Type.SYN.value):
                self.state = State.EST.value
                self.start_time = time.time()
                self.current_seq_no = sender_segment.get_seq_int()
                self.increase_seq_no(1)

                # check if packet is sent during wrong state
                if (self.state == State.TIME_WAIT.value ):
                    logging.warning("syn state reset")
                    receiver_segment = Segment(self.current_seq_no, Type.RESET.value, None, sender_segment.sender_time_sent)
                else:
                    receiver_segment = Segment(self.current_seq_no, Type.ACK.value, None, sender_segment.sender_time_sent)
                    receiver_segment.set_syn_ack(True)

            elif (sender_segment.get_packet_int() == Type.FIN.value):
                # if receiver in time wait state start a new thread which will
                # end the program once reaching MSL*2
                if (self.state != State.TIME_WAIT.value):
                    self.state = State.TIME_WAIT.value
                    self._time_wait_active = True 
                    self.time_wait_timer = time.time()
                    time_wait_thread = Thread(target=self.time_wait_state)
                    time_wait_thread.start()
                    self.increase_seq_no(1)
                    # check if packet is sent during wrong state
                if (self.state == State.LISTEN.value):
                    logging.warning("fin state reset")
                    receiver_segment = Segment(self.current_seq_no, Type.RESET.value, None, sender_segment.sender_time_sent)
                else:
                    receiver_segment = Segment(self.current_seq_no, Type.ACK.value, None, sender_segment.sender_time_sent)
                    receiver_segment.set_fin_ack(True)
                
            else:
                exit()

            # check if packet is dropped from receiver->sender
            if self.is_rlp() and sender_segment.get_packet_int() != Type.RESET.value:
                self.log(receiver_segment, True)
                continue

            self.receiver_socket.sendto(pickle.dumps(receiver_segment), sender_address)
            self.log(receiver_segment, False)

    def is_flp(self):
        if random.random() < float(self.flp):
            return True 
        else:
            return False

    def is_rlp(self):
        if random.random() < float(self.rlp):
            return True 
        else:
            return False
            
    '''
    This function is be run on a separate thread. It will be called
    one the receiver is in a time_wait state 
    '''
    def time_wait_state(self):
        while self._time_wait_active:
            if (time.time() - self.time_wait_timer >= 2):
                break
        self.state = State.CLOSED.value
        self.receiver_socket.close()
        self.f.close()
        os._exit(0)

    '''
    This function is to increase the current sequence number 
    '''
    def increase_seq_no(self, bytes):
        self.current_seq_no += bytes
        if self.current_seq_no > (pow(2,16)-1):
            self.current_seq_no = self.current_seq_no%(pow(2,16)-1)
    
    '''
    This function is to check for any buffered content that can be flushed
    to the receive file
    '''
    def check_buffer_contents(self, segment):
        index = 0
        for s in self.buffer:
            # compare first buffered segment with received segment
            if index == 0:
                if (segment.get_seq_int()+len(segment.data)) == s.get_seq_int():
                    self.f.write(s.data.decode(encoding='UTF-8'))
                    self.increase_seq_no(len(s.data))
                    index += 1
                else:
                    break
            # compare rest of buffer segments with each other
            else:
                prev = self.buffer[index-1]
                if (prev.get_seq_int()+len(segment.data)) == s.get_seq_int():
                    self.f.write(s.data.decode(encoding='UTF-8'))
                    self.increase_seq_no(len(s.data))
                    index += 1
                else:
                    break
        self.buffer = self.buffer[index:]

    def log(self, segment, dropped):
        if segment.get_packet_int() == Type.DATA.value:
            if (dropped):
                logging.warning(f"drp\t{round((time.time()-self.start_time)*1000, 2)}\tDATA\t{segment.get_seq_int()}\t{len(segment.data)}")
            else:
                logging.warning(f"rcv\t{round((time.time()-self.start_time)*1000, 2)}\tDATA\t{segment.get_seq_int()}\t{len(segment.data)}")
        elif segment.get_packet_int() == Type.SYN.value:
            if (dropped):
                logging.warning(f"drp\t-\tSYN\t{segment.get_seq_int()}\t0")
            else:
                logging.warning(f"rcv\t0\tSYN\t{segment.get_seq_int()}\t0")
        elif segment.get_packet_int() == Type.FIN.value:
            if (dropped):
                logging.warning(f"drp\t{round((time.time()-self.start_time)*1000, 2)}\tFIN\t{segment.get_seq_int()}\t0")
            else:
                logging.warning(f"rcv\t{round((time.time()-self.start_time)*1000, 2)}\tFIN\t{segment.get_seq_int()}\t0")
        elif segment.get_packet_int() == Type.ACK.value:
            if (dropped):
                logging.warning(f"drp\t{round((time.time()-self.start_time)*1000, 2)}\tACK\t{segment.get_seq_int()}\t0")
            else:
                logging.warning(f"snd\t{round((time.time()-self.start_time)*1000, 2)}\tACK\t{segment.get_seq_int()}\t0")
        else:
            print("RESET has been sent/received... Program exiting now")


if __name__ == '__main__':
    # logging is useful for the log part: https://docs.python.org/3/library/logging.html
    logging.basicConfig(
        filename="Receiver_log.txt",
        # stream=sys.stderr,
        level=logging.WARNING,
        # format='%(asctime)s,%(msecs)03d %(levelname)-8s %(message)s',
        format='%(message)s',
        datefmt='%Y-%m-%d:%H:%M:%S')

    if len(sys.argv) != 6:
        print(
            "\n===== Error usage, python3 receiver.py receiver_port sender_port FileReceived.txt flp rlp ======\n")
        exit(0)

    receiver = Receiver(*sys.argv[1:])
    receiver.run()