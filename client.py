import redis
import random
from const_chord import (
    LOOKUP_REQ,
)

## This is a test case for the efficacy of the Chord DHT node style - we can do better! 
class Client:
    def __init__(
        self,
        redis_ip='redis',
        redis_port=6379,
        n_bits=5
    ):
        self.channel = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
        self.n_bits = n_bits
        self.MAX_PROC = 2 ** n_bits

    def run(self):
        try:
            client_id = self.MAX_PROC + 1
            print('client id:', client_id)
            self.channel.sadd('members', client_id)
            nodes = [int(i) for i in list(
                self.channel.smembers('members')) if int(i) != client_id]
            nodes.sort()
            if not nodes:
                raise Exception('There is no nodes')
            print(['%04d' % k for k in nodes])

            p = random.choice(nodes)
            key = random.randint(0, self.MAX_PROC - 1)

            print('Sending LOOKUP request for', key, 'to', p)

            self.channel.rpush(f'{client_id}-{p}', f'{LOOKUP_REQ}-{key}')

            message = self.channel.blpop(f'{p}-{client_id}', timeout=0)
            if message:
                print('response:', message[1].decode().split('-')[1])
        except Exception:
            pass
        self.channel.srem('members', client_id)

# We're going to make a flask app to show the state in a web API. 


if __name__ == '__main__':
    c = Client()

    c.run()
