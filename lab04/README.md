## Setup

ZooKeeper is launched in Docker container using `docker-compose up --build` (`docker-compose.yml` contains config).

Connecting: `./bin/zkCli.sh -server localhost:2181`

For this lab I used Python + Kazoo library.

## Tasks

1. ```Tutorial1.ipynb```. Figuring out the API following the assignment instructions.
2. ```Tutorial2_Animals.ipynb```. Implementation of an example with animals.
3. ```philosophers.py```. Dining philosophers problem solution.
4. ```two_phase_commit.py```. 2PC protocol implementation.
