import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class LogEntry:
    term: int
    command: str


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
    prev_log_index: int = -1
    prev_log_term: int = -1
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = -1


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    follower_id: int


class Node:
    def __init__(
        self,
        node_id: int,
        peers: List[int],
        inboxes: Dict[int, asyncio.Queue],
        election_timeout_range=(0.15, 0.3),
        heartbeat_interval=0.01,
        network_delay=(0.01, 0.05),
    ):
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

        self.log: List[LogEntry] = []
        self.commit_index: int = -1
        self.last_applied: int = -1
        self.next_index: Dict[int, int] = {p: 0 for p in peers}  # for leader only
        self.match_index: Dict[int, int] = {p: -1 for p in peers}  # for leader only

    async def send_msg(self, dest_id: int, msg: Any):
        await asyncio.sleep(random.uniform(*self.network_delay))
        await self.inboxes[dest_id].put((self.id, msg))

    async def broadcast(self, msg: Any):
        for p in self.peers:
            asyncio.create_task(self.send_msg(p, msg))

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
        print(
            f"{time.monotonic():.3f} Node {self.id} started election in term {self.current_term} (votes={self.votes_received})"
        )

        rv = RequestVote(
            term=self.current_term,
            candidate_id=self.id,
            last_log_index=0,
            last_log_term=0,
        )
        await self.broadcast(rv)

        election_deadline = time.monotonic() + random.uniform(
            *self.election_timeout_range
        )
        while time.monotonic() < election_deadline and self.state == "candidate":
            if self.votes_received >= self.majority:
                await self.become_leader()
                return
            await asyncio.sleep(0.01)

    async def become_leader(self):
        self.state = "leader"
        print(
            f"{time.monotonic():.3f} Node {self.id} becomes LEADER for term {self.current_term} "
        )

        for p in self.peers:
            self.next_index[p] = len(self.log)
            self.match_index[p] = -1

        if self.leader_task and not self.leader_task.done():
            self.leader_task.cancel()
        self.leader_task = asyncio.create_task(self._leader_heartbeat_loop())

    async def _leader_heartbeat_loop(self):
        while self.state == "leader":
            for p in self.peers:
                prev_index = self.next_index[p] - 1
                prev_term = self.log[prev_index].term if prev_index >= 0 else -1
                entries = self.log[self.next_index[p] :]
                ae = AppendEntries(
                    term=self.current_term,
                    leader_id=self.id,
                    prev_log_index=prev_index,
                    prev_log_term=prev_term,
                    entries=entries,
                    leader_commit=self.commit_index,
                )
                asyncio.create_task(self.send_msg(p, ae))
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
                resp = AppendEntriesResponse(
                    term=self.current_term, success=True, follower_id=self.id
                )
                asyncio.create_task(self.send_msg(sender_id, resp))
            elif isinstance(msg, AppendEntriesResponse):
                await self._handle_append_entries_response(sender_id, msg)
            else:
                print(f"Node {self.id} got unknown message: {msg}")

    async def _handle_request_vote(self, sender_id: int, req: RequestVote):
        if req.term < self.current_term:
            resp = RequestVoteResponse(
                term=self.current_term, vote_granted=False, voter_id=self.id
            )
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
            resp = RequestVoteResponse(
                term=self.current_term, vote_granted=True, voter_id=self.id
            )
            print(
                f"{time.monotonic():.3f} Node {self.id} voates for {req.candidate_id} in term {req.term}"
            )
            await self.send_msg(sender_id, resp)
        else:
            resp = RequestVoteResponse(
                term=self.current_term, vote_granted=False, voter_id=self.id
            )
            await self.send_msg(sender_id, resp)

    async def _handle_request_vote_response(
        self, sender_id: int, resp: RequestVoteResponse
    ):
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
            print(
                f"{time.monotonic():.3f} Node {self.id} received vote from {resp.voter_id} (total={self.votes_received})"
            )

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
                print(
                    f"{time.monotonic():.3f} Node {self.id} stepped down to follower due to higher term {ae.term}"
                )

        if ae.prev_log_index >= 0:
            if (
                len(self.log) <= ae.prev_log_index
                or self.log[ae.prev_log_index].term != ae.prev_log_term
            ):
                resp = AppendEntriesResponse(
                    term=self.current_term, success=False, follower_id=self.id
                )
                asyncio.create_task(self.send_msg(sender_id, resp))
                return

        self.log = self.log[: ae.prev_log_index + 1] + ae.entries

        if ae.leader_commit > self.commit_index:
            old_commit_index = self.commit_index
            self.commit_index = min(ae.leader_commit, len(self.log) - 1)
            for i in range(old_commit_index + 1, self.commit_index + 1):
                self.last_applied += 1
                self.apply(self.log[i])

    def apply(self, command):
        print(f"{time.monotonic():.3f} Node {self.id} applies: {command}")

    async def _handle_append_entries_response(
        self, sender_id: int, resp: AppendEntriesResponse
    ):
        if resp.term > self.current_term:
            self.current_term = resp.term
            self.state = "follower"
            self.voted_for = None
            self.election_event.set()
            return

        if self.state != "leader":
            return

        if resp.success:
            self.match_index[sender_id] = max(
                self.match_index[sender_id],
                self.next_index[sender_id]
                + len(self.log[self.next_index[sender_id] :])
                - 1,
            )
            self.next_index[sender_id] = self.match_index[sender_id] + 1

            for i in range(self.commit_index + 1, len(self.log)):
                count = 1
                for p in self.peers:
                    if self.match_index[p] >= i:
                        count += 1
                if count >= self.majority and self.log[i].term == self.current_term:
                    self.commit_index = i
        else:
            self.next_index[sender_id] = max(0, self.next_index[sender_id] - 1)


async def run_simulation(
    node_count=5,
    runtime=5.0,
    seed=None,
    election_time_range=(0.15, 0.3),
    network_delay=(0.01, 0.05),
):

    if seed is not None:
        random.seed(seed)
    inboxes = {i: asyncio.Queue() for i in range(node_count)}
    nodes = {}
    for i in range(node_count):
        peers = [j for j in range(node_count) if j != i]
        n = Node(
            node_id=i,
            peers=peers,
            inboxes=inboxes,
            election_timeout_range=election_time_range,
            heartbeat_interval=0.05,
            network_delay=network_delay,
        )
        nodes[i] = n

    for n in nodes.values():
        await n.start()

    await asyncio.sleep(2)
    leader = next((n for n in nodes.values() if n.state == "leader"), None)
    if leader:
        for i in range(10):
            print(
                f"\n{time.monotonic():.3f} Client: Leader {leader.id} appends command x={i}\n"
            )
            leader.log.append(LogEntry(term=leader.current_term, command=f"x={i}"))
            await asyncio.sleep(0.1)

    print(f"Simulation start: {node_count} nodes, runtime {runtime}s")
    await asyncio.sleep(runtime)
    print("Simulation end")


asyncio.run(run_simulation(node_count=5, runtime=6.0, seed=12345))
