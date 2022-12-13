import time
import threading
from pysqream.SQSocket import Client


class PingLoop(threading.Thread):
    def __init__(self, conn):
        self.conn = conn
        self.client = Client(self.conn)
        super(PingLoop, self).__init__()
        self.done = False

    def run(self):
        json_cmd = '{"ping":"ping"}'
        binary = self.client.generate_message_header(len(json_cmd)) + json_cmd.encode('utf8')
        while self.sleep():
            conn = self.conn
            try:
                conn.s.send(binary)
            except:
                self.done = True

    def halt(self):
        self.done = True

    def sleep(self):
        if self.done:
            return False
        count = 0
        while (count < 100):
            count = count + 1
            time.sleep(.1)
            if self.done:
                return False
        return True


def _start_ping_loop(conn):
    ping_loop = PingLoop(conn)
    ping_loop.start()
    return ping_loop


def _end_ping_loop(ping_loop):
    if ping_loop is not None:
        ping_loop.halt()
        ping_loop.join()