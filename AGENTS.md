# AGENTS.md (my-raft)

## Entry point / build
- Spring Boot 3.3.5, Java 21, Maven. App: `com.myraft.MyRaftApplication`.
- Build: `mvn clean package -DskipTests` (Maven not on Windows PATH; use IntelliJ bundle: `"C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd"` or from ASCII copy in `C:\temp\myraft-build`).
- Run tests: `mvn test` — 68 tests in 17 classes, BUILD SUCCESS. Single test: `mvn test -Dtest=ClassName#methodName`. Details in `docs/TEST-CONTEXT.md`; main classes: `MyRaftApplicationTests`, `api.RaftClientApiTest`, `api.StatusResponseJsonTest`, `raft.RaftElectionTest`, `raft.RaftNodeElectionTest`, `raft.RaftStateAtomicityTest`, `raft.RaftNodeApplyTest`, `raft.RaftReplicationTest`, `raft.rpc.*`, `raft.model.*`, `raft.fsm.kv.KvStateMachineTest`, `raft.storage.*`.
- JAR: `target/my-raft-*.jar` (Spring Boot repackaged). `start-node.cmd` uses existing JAR or builds if missing; if source/config changed, JAR **must** be rebuilt.

## Runtime (Windows only)
- Scripts only `.cmd`, args only. Each node in its own console window; closing window stops node.
- `scripts/runtime/start-node.cmd <node> [total-nodes] [base-port] [--jvm "..."]` — primitive (accepts `2` or `node-2`). Resolves real JDK via `JAVA_HOME` or `java.home` (avoid Oracle `javapath` shim). Checks port only for `LISTENING`. Builds peer list; runs with `-D... -jar` before start. Adds default JVM flag `-XX:TieredStopAtLevel=1` (C1-only JIT) before extra `--jvm` flags, so extra flags can override it.
- `scripts/runtime/start-cluster.cmd [total-nodes] [base-port] [--jvm "..."]` — loop over `start-node.cmd` (single source of truth).
- `scripts/runtime/stop-cluster.cmd` — kills `java.exe` with `my-raft*.jar` in command line.
- `scripts/runtime/build-jar.cmd` — builds artifact from project root (`mvn -DskipTests package`).
- Defaults: 3 nodes on 8080/8081/8082. Test ports: 18080-18082. `DATA_DIR` defaults to `data/` but is **not created by runtime scripts**; in in-memory mode no directory/files appear. In file mode `FileRaftStorage` creates the parent directory on first persist (`node-N.json`).
- Storage mode: in-memory by default (state is lost on restart); file mode via `-Draft.storage.type=file` (`raft.storage-path`, e.g. `data/node-N.json`).
- Runtime scripts support `--dry-run` (prints the exact `java` command instead of starting node(s)); extra JVM flags: `--jvm "-Dkey=value ..."`.
- Startup timing (3 nodes, 18080-18082): ~39.6 s to all `/status` before flags, ~20.2 s with `-XX:TieredStopAtLevel=1` (~2× faster). Removing unused `@EnableScheduling` and disabling banner/JMX had no measurable effect on timing.

## WSL ↔ Windows gotchas (important)
- Path has spaces/cyrillic: prefer short path `C:\Users\BCC9~1\IdeaProjects\my-raft` or ASCII copy `C:\temp\myraft-build`.
- Run Windows commands from Windows dir: `cmd.exe /c "cd /d C:\temp && ..."` or `powershell.exe -NoProfile -Command "Set-Location 'C:\temp'; ..."` to avoid UNC issues.
- Maven from WSL: use `powershell.exe -NoProfile -Command "Set-Location 'C:\temp\myraft-build'; & '...\\mvn.cmd' test"` (avoid nested `cmd.exe /c` chains).
- `cmd.exe` launched from WSL may not return while child windows started by `start` are open (console handle inheritance).
- `java` in PATH is Oracle `javapath` shim — scripts resolve real JDK.

## Testing
- Unit/integration in-memory: `src/test/` — 68 tests in 17 classes (see above). Run via Maven (`mvn test`); coverage is described in `docs/TEST-CONTEXT.md`.
- Manual cluster check is not part of `mvn test`: commands for test ports 18080–18082 are in `docs/ENVIRONMENT.md` (§10). Rebuild the JAR when sources/config changed.
- Port checks ignore `TIME_WAIT`. No Docker/gRPC.

## Key implementation notes
- Raft core in `src/main/java/com/myraft/raft/` (election, heartbeats, log replication/commit, RPC, storage, FSM). Snapshots, pre-vote and dynamic membership are not implemented.
- Persisted Raft state: `formatVersion=2`, `currentTerm`, `votedFor` and the replicated log (`PersistedState`); empty/missing file state starts at term 0, votedFor null. `commitIndex`/`lastApplied`/role/`leaderId` are volatile.
- Term/vote transitions are atomic under `RaftState` write-lock with persist-before-visible: `startElection(selfId)` increments term and self-votes, `grantVote(candidateTerm, candidateId)` decides/grants at most one vote per term, `updateTermAndVote` is monotonic (`term < currentTerm` is no-op). `RaftRpcService`/`RaftNode` use these APIs to avoid double-vote and term rollback (F1/F2 fixes).
- Timeouts (current `application.yml` defaults): election 500-700ms, heartbeat 100ms, `RestTemplate` connect/read 200ms, client apply wait 5000ms. `ElectionTimer` uses epoch + sync to drop stale timeouts.
- Concurrency model: single `nodeLock` in `RaftNode` serializes all node transitions (`role`/`leaderId`, election timer, heartbeats, inbound RPC, RPC response callbacks); lock order is `nodeLock` → `RaftState` lock (inner state lock is not exposed). Network calls run outside `nodeLock`.
- Inbound RPC: `RaftNode.handleRequestVote/handleAppendEntries` return `RequestVoteOutcome`/`AppendEntriesOutcome` (event + `StateTransition` of `RaftStateSnapshot`s). `RaftRpcService` is a thin adapter: snapshot prefilters (stale term, already voted, foreign leader at same term), `GRANTED_REPEAT` classification, logs the event, builds the response from `after.currentTerm()`; it does not re-read live state. On `GRANTED_REPEAT` the service asks `RaftNode.resetElectionTimerIfUnchanged(after)` to reset the timer under `nodeLock`. `AppendEntries` at equal term from another leader (current `leaderId != null`) is rejected with `REJECTED_FOREIGN_LEADER`.
- Logs: readable console format with MDC `node/term/role`; events include `starting election`, `RequestVote sent term=... peers=N` (once per election round; per-peer lines are DEBUG in `RaftRpcClient`), `vote granted`, `became leader`, `stepping down`, `vote rejected`, `leader changed`/`discovered new leader`, `AppendEntries sent` (once per round on the leader), `AppendEntries received`, and heartbeat accepted/ignored (heartbeats are empty `AppendEntries`).
- API: `GET /status` returns `isLeader` (JSON property); `POST /raft/log` (leader only, waits for commit/apply, `503 + leaderId`, `504` on timeout), `GET /raft/log` (user entries without NoOp), `GET /raft/kv?key=...` (`found` separates missing key from JSON `null`). Peers filtered for blanks.

## Git
- Не коммитить и не пушить изменения в git без явного запроса пользователя.
- Любые git-операции (`add`, `commit`, `push`, `pull`, rebase/merge/tag и т.п.) выполнять только по явному запросу.
