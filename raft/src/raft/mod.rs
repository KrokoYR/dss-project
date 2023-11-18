use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::executor::ThreadPool;
use futures::stream::StreamExt;
use futures::task::SpawnExt;
use futures::{select, FutureExt};
use futures_timer::Delay;

use rand::{thread_rng, Rng};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use labrpc::Result as RpcResult;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

pub enum Event {
    ResetElectionTimeout,
    ElectionTimeout,
    HeartbeatTimeout,
    VoteReceived((u64, RequestVoteReply)),
    AppendEntriesReceived((u64, AppendEntriesReply)),
}

/// Implementatin of proto reply structs
///
impl RequestVoteReply {
    pub fn new(term: u64, vote_granted: bool) -> Self {
        Self { term, vote_granted }
    }
}

impl AppendEntriesReply {
    pub fn new(term: u64, success: bool) -> Self {
        Self { term, success }
    }
}

#[derive(Eq, PartialEq)]
pub enum Role {
    Follower = 1,
    Candidate = 2,
    Leader = 3,
}

#[derive(Debug, Default)]
pub struct Log {
    log: Vec<LogEntry>,
}

/// Custom implementation of log, because first index is 1
impl Log {
    pub fn first(&self) -> Option<&LogEntry> {
        self.log.get(0)
    }

    pub fn get(&self, idx: usize) -> Option<&LogEntry> {
        self.log.get(idx - 1)
    }

    pub fn last_idx(&self) -> u64 {
        if self.log.len() == 0 {
            0
        } else {
            1 + self.log.len() as u64
        }
    }

    pub fn last_term(&self) -> u64 {
        if self.log.len() == 0 {
            0
        } else {
            self.log.last().unwrap().term
        }
    }
}

pub struct CandidateState {
    received_votes: u64,
}

pub struct LeaderState {
    next_index: Vec<u64>,
    match_index: Vec<u64>,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,

    curr_term: u64,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    voted_for: Option<u64>,

    log: Log,

    role: Role,

    event_tx: Option<UnboundedSender<Event>>,

    candidate_state: Option<CandidateState>,

    leader_state: Option<LeaderState>,

    apply_ch: UnboundedSender<ApplyMsg>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            curr_term: 0,
            voted_for: None,
            log: Log::default(),
            role: Role::Follower,
            event_tx: None,
            candidate_state: None,
            leader_state: None,
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf.to_follower(rf.curr_term);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(&self, server: usize, args: RequestVoteArgs) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let event_tx = self.event_tx.as_ref().unwrap().clone();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            if let Ok(reply) = res {
                event_tx
                    .unbounded_send(Event::VoteReceived((server as u64, reply)))
                    .unwrap();
            }
        });
    }

    fn send_append_entries(&self, server: usize, args: AppendEntriesArgs) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let event_tx = self.event_tx.as_ref().unwrap().clone();

        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            if let Ok(reply) = res {
                event_tx
                    .unbounded_send(Event::AppendEntriesReceived((server as u64, reply)))
                    .unwrap();
            }
        });
    }

    fn heartbeat_args(&self) -> AppendEntriesArgs {
        AppendEntriesArgs {
            term: self.curr_term,
            leader_id: self.me as u64,
            prev_log_index: self.log.last_idx(),
            prev_log_term: self.log.last_term(),
            entries: vec![],
            leader_commit: 0,
        }
    }

    fn send_heartbeats(&self) {
        for p in self.peers() {
            self.send_append_entries(p, self.heartbeat_args());
        }
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }

    /// Reset candidate and leader states to None
    fn reset_states(&mut self) {
        self.candidate_state = None;
        self.leader_state = None;
    }

    fn to_follower(&mut self, new_term: u64) {
        self.reset_states();

        self.role = Role::Follower;
        self.curr_term = new_term;
        self.voted_for = None;
    }

    fn to_candidate(&mut self) {
        self.reset_states();

        self.role = Role::Candidate;
        self.curr_term += 1;
        self.voted_for = Some(self.me as u64);
        self.candidate_state = Some(CandidateState { received_votes: 1 });
        info!("[{}] -> candidate, term={}", self.me, self.curr_term);
    }

    fn to_leader(&mut self) {
        self.reset_states();

        self.role = Role::Leader;
        self.leader_state = Some(LeaderState {
            next_index: vec![self.log.last_idx() + 1; self.peers.len()],
            match_index: vec![0; self.peers.len()],
        });
    }

    fn peers(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.peers.len()).filter(move |p| *p != self.me)
    }

    fn incr_vote(&mut self) {
        self.candidate_state.as_mut().unwrap().received_votes += 1;
    }

    fn is_majority(&self, num: u64) -> bool {
        num >= (self.peers.len() / 2 + 1) as u64
    }

    fn start_election(&mut self) {
        self.to_candidate();

        let last_log_index = self.log.last_idx();
        let last_log_term = self.log.last_term();

        let peers = self.peers();
        for p in peers {
            self.send_request_vote(
                p,
                RequestVoteArgs {
                    term: self.curr_term,
                    candidate_id: self.me as u64,
                    last_log_index,
                    last_log_term,
                },
            );
        }
    }

    pub fn handle_event(&mut self, event: Event) {
        match event {
            Event::ResetElectionTimeout => unreachable!(),
            Event::ElectionTimeout => {
                if self.role != Role::Leader {
                    self.start_election();
                }
            }
            Event::HeartbeatTimeout => {
                if self.role == Role::Leader {
                    self.send_heartbeats();
                }
            }
            Event::VoteReceived((from, reply)) => {
                let RequestVoteReply { term, vote_granted } = reply;

                match term.cmp(&self.curr_term) {
                    Ordering::Greater => self.to_follower(term),
                    Ordering::Equal if vote_granted && self.role == Role::Candidate => {
                        self.incr_vote();

                        if self.is_majority(self.candidate_state.as_ref().unwrap().received_votes) {
                            self.to_leader();
                            self.send_heartbeats();
                        }
                    }
                    _ => {}
                }
            }
            Event::AppendEntriesReceived((from, reply)) => {
                if reply.term > self.curr_term {
                    self.to_follower(reply.term);
                }
            }
        }
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RpcResult<RequestVoteReply> {
        if args.term > self.curr_term {
            self.to_follower(args.term);
        }

        let vote_granted = self
            .voted_for
            .map_or(true, |v| v as u64 == args.candidate_id);

        if vote_granted {
            self.voted_for = Some(args.candidate_id);
        }

        Ok(RequestVoteReply::new(self.curr_term, vote_granted))
    }

    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> RpcResult<AppendEntriesReply> {
        if args.term > self.curr_term {
            self.to_follower(args.term);
        }

        if args.term < self.curr_term {
            Ok(AppendEntriesReply::new(self.curr_term, false))
        } else {
            self.event_tx
                .as_ref()
                .unwrap()
                .unbounded_send(Event::ResetElectionTimeout)
                .unwrap();

            Ok(AppendEntriesReply::new(self.curr_term, true))
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        self.persist();
        let _ = &self.persister;
        let _ = &self.apply_ch;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    pub executor: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Self {
        let (event_tx, event_rx) = unbounded();
        raft.event_tx = Some(event_tx.clone());

        // Your code here.
        let mut node = Self {
            raft: Arc::new(Mutex::new(raft)),
            executor: ThreadPool::new().unwrap(),
        };
        node.start_event_loop(event_tx, event_rx);

        node
    }

    pub fn start_event_loop(
        &mut self,
        event_tx: UnboundedSender<Event>,
        mut event_rx: UnboundedReceiver<Event>,
    ) {
        let raft = Arc::clone(&self.raft);

        let reset_election_timeout =
            || Delay::new(Duration::from_millis(thread_rng().gen_range(150..300))).fuse();
        let mut election_timeout = reset_election_timeout();

        let reset_heartbeat_timeout =
            || futures_timer::Delay::new(Duration::from_millis(100)).fuse(); // select! requires FuseFuture
        let mut heartbeat_timeout = reset_heartbeat_timeout();

        self.executor
            .spawn(async move {
                loop {
                    select! {
                        e = event_rx.select_next_some() => {
                            match e {
                                Event::ResetElectionTimeout => {
                                    election_timeout = reset_election_timeout();
                                },
                                _ => {
                                    raft.lock().unwrap().handle_event(e);
                                }
                            }
                        },
                        _ = election_timeout => {
                            event_tx.unbounded_send(Event::ElectionTimeout).unwrap();
                            election_timeout = reset_election_timeout();
                        },
                        _ = heartbeat_timeout => {
                            event_tx.unbounded_send(Event::HeartbeatTimeout).unwrap();
                            heartbeat_timeout = reset_heartbeat_timeout();
                        }

                    }
                }
            })
            .unwrap();
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft.lock().unwrap().curr_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft.lock().unwrap().role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    // TODO: check, if network is being actually jammed
    async fn request_vote(&self, args: RequestVoteArgs) -> RpcResult<RequestVoteReply> {
        self.raft.lock().unwrap().handle_request_vote(args)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> RpcResult<AppendEntriesReply> {
        self.raft.lock().unwrap().handle_append_entries(args)
    }
}
