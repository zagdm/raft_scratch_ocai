# AGENTS.md (my-raft)

## Entry point / build
- Spring Boot 3.3.5, Java 21, Maven. App: `com.myraft.MyRaftApplication`.
- Build: `mvn clean package -DskipTests` (Maven not on Windows PATH; use IntelliJ bundle: `"C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd"` or from ASCII copy in `C:\temp\myraft-build`).
- Run tests: `mvn test`. Single test: `mvn test -Dtest=ClassName#methodName`. All 7 unit tests pass.
- JAR: `target/my-raft-*.jar` (Spring Boot repackaged). `start-node.cmd` uses existing JAR or builds if missing; if source/config changed, JAR **must** be rebuilt.

## Runtime (Windows only)
- Scripts only `.cmd`, args only. Each node in its own console window; closing window stops node.
- `scripts/runtime/start-node.cmd <node> [total-nodes] [base-port]` — primitive (accepts `2` or `node-2`). Resolves real JDK via `JAVA_HOME` or `java.home` (avoid Oracle `javapath` shim). Checks port only for `LISTENING`. Builds peer list; runs with `-D... -jar` before start.
- `scripts/runtime/start-cluster.cmd [total-nodes] [base-port]` — loop over `start-node.cmd` (single source of truth).
- `scripts/runtime/stop-cluster.cmd` — kills `java.exe` with `my-raft*.jar` in command line.
- `scripts/runtime/build-jar.cmd` — builds artifact from project root (`mvn -DskipTests package`).
- Defaults: 3 nodes on 8080/8081/8082. Test ports: 18080-18082. `DATA_DIR` defaults to `data/` (state `node-N.json`), created if missing.

## WSL ↔ Windows gotchas (important)
- Path has spaces/cyrillic: prefer short path `C:\Users\BCC9~1\IdeaProjects\my-raft` or ASCII copy `C:\temp\myraft-build`.
- Run Windows commands from Windows dir: `cmd.exe /c "cd /d C:\temp && ..."` to avoid UNC issues.
- Maven from WSL: use `powershell.exe -NoProfile -Command "Set-Location 'C:\temp\myraft-build'; & '...\\mvn.cmd' test"` (avoid nested `cmd.exe /c` chains).
- `cmd.exe` launched from WSL may not return while child windows started by `start` are open (console handle inheritance).
- `java` in PATH is Oracle `javapath` shim — scripts resolve real JDK.

## Testing
- Unit: `src/test/` — 7 tests (StatusResponseJsonTest, RaftElectionTest, RaftNodeElectionTest, FileRaftStorageTest). Run via Maven.
- Integration (infrastructure): follow `TEST-CONTEXT.md`. Order: stop cluster, clean `data/`, `scripts/runtime/build-jar.cmd` (if sources/config changed), start from foreign cwd (`C:\temp`) via `start-cluster.cmd`, close node window (`CloseMainWindow`), restore via `start-node.cmd` from foreign cwd, verify all `/status`, then stop and clean `data/`.
- Port checks ignore `TIME_WAIT`. No Docker/gRPC.

## Key implementation notes
- Raft core in `src/main/java/com/myraft/raft/` (election, heartbeats, RPC, storage). Only `currentTerm`/`votedFor` persisted (`FileRaftStorage`). Empty/missing state starts at term 0.
- Timeouts: election 500-800ms, heartbeat 100ms, RPC 200ms (tuned for stability). `ElectionTimer` uses epoch + sync to drop stale timeouts.
- Logs: readable format with MDC `node/term/role`, heartbeats/status often DEBUG/TRACE to reduce noise.
- API: `GET /status` returns `isLeader` (JSON property). Peers filtered for blanks.
