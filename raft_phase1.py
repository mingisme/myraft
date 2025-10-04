import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any,Dict,List,Optional

@dataclass
class RequestVote:
    term: int
    candidate_id: int
    last_log_index: int = 0
    last_log_term: int = 0

@dataclass
class RequestVoteResponse:  
    term: int
    vote_granted: bool
    voter_id: int

@dataclass
class AppendEntries:
    term: int
    leader_id: int
    

@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    follower_id: int

class Node:
    def __init__(self, node_id: int, peers: List[int], inboxes: Dict[int,asyncio.Queue], 
    election_timeout_range=(0.15,0.3), heartbeat_interval=0.01, network_delay=(0.01,0.05)):
        self.id = node_id
        self.peers = peers
        self.inboxes = inboxes
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.state = "follower"
        self.votes_received = 0

        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self.network_delay = network_delay

        self.election_event = asyncio.Event()
        self.election_task: Optional[asyncio.Task] = None
        self.leader_task: Optional[asyncio.Task] = None

        self.inbox = inboxes[self.id]

        self.total_nodes = len(peers) + 1
        self.majority = self.total_nodes // 2 + 1

        self._last_log = []

    async def send_msg(self, dest_id: int, msg: Any):
        await asyncio.sleep(random.uniform(*self.network_delay))
        await self.inboxes[dest_id].put((self.id,msg))

    async def broadcast(self, msg: Any):
        for p in self.peers:
            asyncio.create_task(self.send_msg(p,msg))

    async def start(self):
        self.election_task = asyncio.create_task(self._run_election_timer())
        asyncio.create_task(self._run_message_loop())

    async def _run_election_timer(self):
        while True:
            self.election_event.clear()
            timeout = random.uniform(*self.election_timeout_range)
            try:
                await asyncio.wait_for(self.election_event.wait(), timeout)
                continue
            except asyncio.TimeoutError:
                asyncio.create_task(self.start_election())

    async def start_election(self):
        self.current_term += 1
        self.state = "candidate"
        self.voted_for = self.id
        self.votes_received = 1
        print(f"{time.monotonic():.3f} Node {self.id} started election in term {self.current_term} (votes={self.votes_received})")

        rv = RequestVote(term=self.current_term,candidate_id = self.id,last_log_index = 0, last_log_term = 0)
        await self.broadcast(rv)

        election_deadline = time.monotonic() + random.uniform(*self.election_timeout_range)
        while time.monotonic() < election_deadline and self.state == "candidate":
            if self.votes_received >= self.majority:
                await self.become_leader()
                return
            await asyncio.sleep(0.01)

    
    async def become_leader(self):
        self.state = "leader"
        print(f"{time.monotonic():.3f} Node {self.id} becomes LEADER for term {self.current_term} ")

        if self.leader_task and not self.leader_task.done():
            self.leader_task.cancel()
        self.leader_task = asyncio.create_task(self._leader_heartbeat_loop())

    async def _leader_heartbeat_loop(self):
        while self.state == "leader":
            ae = AppendEntries(term = self.current_term, leader_id = self.id)
            await self.broadcast(ae)
            await asyncio.sleep(self.heartbeat_interval)


    async def _run_message_loop(self):
        while True:
            sender_id, msg = await self.inbox.get()
            if isinstance(msg, RequestVote):
                await self._handle_request_vote(sender_id, msg)
            elif isinstance(msg, RequestVoteResponse):
                await self._handle_request_vote_response(sender_id, msg)
            elif isinstance(msg, AppendEntries):
                await self._handle_append_entries(sender_id, msg)
            elif isinstance(msg, AppendEntriesResponse):
                pass
            else:
                print(f"Node {self.id} got unknown message: {msg}")

    
    async def _handle_request_vote(self, sender_id: int, req: RequestVote):
        if req.term < self.current_term:
            resp = RequestVoteResponse(term = self.current_term, vote_granted=False, voter_id = self.id)
            await self.send_msg(sender_id, resp)
            return

        if req.term > self.current_term:
            self.current_term = req.term
            self.state = "follower"
            self.voted_for = None

        can_vote = self.voted_for is None or self.voted_for == req.candidate_id
        if can_vote:
            self.voted_for = req.candidate_id
            self.election_event.set()
            resp = RequestVoteResponse(term = self.current_term, vote_granted = True, voter_id = self.id)
            print(f"{time.monotonic():.3f} Node {self.id} voates for {req.candidate_id} in term {req.term}")
            await self.send_msg(sender_id, resp)
        else:
            resp = RequestVoteResponse(term = self.current_term, vote_granted = False, voter_id = self.id)
            await self.send_msg(sender_id, resp)

    async def _handle_request_vote_response(self, sender_id: int, resp: RequestVoteResponse):
        if resp.term > self.current_term:
            self.current_term = resp.term
            self.state = "follower"
            self.voted_for = None
            self.election_event.set()
            return

        if self.state != "candidate":
            return

        
        if resp.vote_granted:
            self.votes_received += 1
            print(f"{time.monotonic():.3f} Node {self.id} received vote from {resp.voter_id} (total={self.votes_received})")
        else:
            pass

    async def _handle_append_entries(self, sender_id: int, ae: AppendEntries):
        if ae.term < self.current_term:
            return

        if ae.term >= self.current_term:
            if ae.term > self.current_term:
                self.current_term = ae.term

            prev_state = self.state
            self.state = "follower"
            self.voted_for = None
            self.election_event.set()
            if prev_state == "leader" and self.state != "leader":
                print(f"{time.monotonic():.3f} Node {self.id} stepped down to follower due to higher term {ae.term}")



async def run_simulation(node_count = 5, runtime = 5.0, seed = None, election_time_range=(0.15,0.3),
            network_delay = (0.01,0.05)):

            if seed is not None: 
                random.seed(seed)
            inboxes = {i: asyncio.Queue() for i in range(node_count)}
            nodes = {}
            for i in range(node_count):
                peers = [j for j in range(node_count) if j!=i]
                n = Node(node_id=i, peers=peers, inboxes = inboxes, election_timeout_range=election_time_range,
                        heartbeat_interval = 0.05, network_delay=network_delay)
                nodes[i] = n

            for n in nodes.values():
                await n.start()


            print(f"Simulation start: {node_count} nodes, runtime {runtime}s")
            await asyncio.sleep(runtime)
            print("Simulation end")


asyncio.run(run_simulation(node_count=5, runtime=6.0, seed=12345))


