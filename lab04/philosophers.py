import logging

from time import time, sleep
from threading import Thread
from enum import Enum

from kazoo.client import KazooClient

logging.basicConfig()

NAMESPACE = '/philosophers_task'
FORKS_URL = NAMESPACE + '/forks'

N = 5

SHOW_FORKS_STATUSES = True
HIDE_STATUS_SPAM = True


class PhilosopherStatus(Enum):
    THINKING = 1
    EATING = 2


class Philosopher(Thread):

    def __init__(self, id, lifetime=10, eattime=1):
        super().__init__()
        self.id = id
        self.l_fork = id
        self.r_fork = (id + 1) % N
        self.lifetime = lifetime
        self.eattime = eattime
        self.eatings_count = 0
        self.status = PhilosopherStatus.THINKING

    def update_status(self, new_status: PhilosopherStatus):
        if (not HIDE_STATUS_SPAM) or self.status != new_status:
            self.status = new_status
            print(f'Ph. #{self.id}: {self.status.name}')

    # @Override
    def run(self):
        zk = KazooClient()
        zk.start()

        forks_semaphore = zk.Lock(FORKS_URL, self.id)
        l_fork_semaphore = zk.Lock(f'{FORKS_URL}/{self.l_fork}', self.id)
        r_fork_semaphore = zk.Lock(f'{FORKS_URL}/{self.r_fork}', self.id)

        start_time = time()

        # Philosopher's lifecycle
        while time() < start_time + self.lifetime:

            # philosopher starts thinking
            self.update_status(PhilosopherStatus.THINKING)
            # print(f'Ph. #{self.id}: THINKING')

            # philosopher tries to access the table with the forks
            # (only one philosopher at a time, therefore 'forks_semaphore' is used)
            with forks_semaphore:
                # if the correct forks are free to acquire, he does so
                if len(l_fork_semaphore.contenders()) == 0 and len(r_fork_semaphore.contenders()) == 0:
                    l_fork_semaphore.acquire()
                    r_fork_semaphore.acquire()
                    if SHOW_FORKS_STATUSES:
                        print(f'FORKS #{self.l_fork} and #{self.r_fork} were acquired by Ph. #{self.id}')

            # if the philosopher was successful enough to gain both forks, he starts eating
            if l_fork_semaphore.is_acquired and r_fork_semaphore.is_acquired:
                self.update_status(PhilosopherStatus.EATING)
                # print(f'Ph. #{self.id}: EATING')
                self.eatings_count += 1
                # eating
                sleep(self.eattime)
                # eating is done
                l_fork_semaphore.release()
                r_fork_semaphore.release()
                if SHOW_FORKS_STATUSES:
                    print(f'FORKS #{self.l_fork} and #{self.r_fork} were released by Ph. #{self.id}')

        print(f'Ph. #{self.id}: ENDS, total eatings: {self.eatings_count}')

        zk.stop()
        zk.close()


if __name__ == '__main__':

    global_zk = KazooClient()
    global_zk.start()

    if global_zk.exists(NAMESPACE):
        global_zk.delete(NAMESPACE, recursive=True)

    global_zk.create(NAMESPACE)
    global_zk.create(FORKS_URL)

    for i in range(N):
        global_zk.create(f'{FORKS_URL}/{i}')

    for i in range(N):
        ph = Philosopher(i)
        ph.start()

    # Asymmetric variant
    #
    # for i in range(N-1):
    #     ph = Philosopher(i)
    #     ph.start()
    #
    # Philosopher(N-1, lifetime=20, eattime=5).start()
