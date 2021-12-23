import logging
import random
import threading
from time import sleep

from kazoo.client import KazooClient

logging.basicConfig()


COMMIT = b'commit'
ABORT = b'abort'


class Client(threading.Thread):
    def __init__(self, root: str, id: int, ttl=5):
        super().__init__()
        self.root = root
        self.id = id
        self.url = f'{root}/{id}'
        self.ttl = ttl

    # @Override
    def run(self):
        zk = KazooClient()
        zk.start()

        value = COMMIT if random.random() > 0.5 else ABORT
        print(f'Client {self.id} requests: {value.decode()}')
        zk.create(self.url, value, ephemeral=True)

        @zk.DataWatch(self.url)
        def watch_myself(data, stat):
            if stat.version != 0:
                print(f'Client {self.id} verdict was set to: {data.decode()}')

        sleep(self.ttl)

        zk.stop()
        zk.close()


class Coordinator:

    def __init__(self, root: str, n_clients: int, timeout: int, strict_decision=False):
        self.strict_decision = strict_decision
        self.timeout = timeout
        self.n_clients = n_clients
        self.root = root

        self.zk = KazooClient()
        self.zk.start()
        if self.zk.exists(self.root):
            self.zk.delete(self.root, recursive=True)
        self.zk.create(self.root)

        # inactivity timer
        self.timer = None

        @self.zk.ChildrenWatch(root)
        def watch_clients(clients):

            # cancel timer if something changed
            if self.timer is not None:
                self.timer.cancel()

            if len(clients) == 0:
                print('No one is present...')
            else:
                # if there are some clients, start the inactivity timer (after timeout the decision will be made)
                self.timer = threading.Timer(self.timeout, lambda: self._decide())
                self.timer.daemon = True
                self.timer.start()

                if len(clients) < self.n_clients:
                    print('Waiting for others...', clients)
                    # timer can kick off at any moment...

                elif len(clients) == self.n_clients:
                    # everyone onboard, cancel the timer and call decision function manually
                    self.timer.cancel()
                    self._decide()

    def _decide(self):
        print('Checking votes and deciding')
        clients = self.zk.get_children(self.root)

        votes = [self.zk.get(f'{self.root}/{client}')[0] for client in clients]
        aborts = votes.count(ABORT)
        commits = votes.count(COMMIT)

        if self.strict_decision:
            decision = ABORT if aborts > 0 else COMMIT
        else:
            decision = COMMIT if commits > aborts else ABORT

        for client in clients:
            self.zk.set(f'{self.root}/{client}', decision)

    def main(self):

        for i in range(self.n_clients):
            p = Client(self.root, i)
            p.start()


if __name__ == '__main__':
    coordinator = Coordinator(root='/2PC', n_clients=5, timeout=20)
    coordinator.main()
