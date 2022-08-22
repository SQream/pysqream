import time
import threading


class PingLoop(threading.Thread):
    def __init__(self, conn):
        self.conn = conn
        super(PingLoop, self).__init__()
        self.done = False

    def run(self):
        json_cmd = '{"ping":"ping"}'
        binary = self.conn.s.generate_message_header(len(json_cmd)) + json_cmd.encode('utf8')
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