# gRPC Binary Lifecycle Manager

Kyron Runtime 기반의 프로덕션급 바이너리 프로세스 라이프사이클 관리 시스템입니다.

## 개요

Pullpiri의 컨테이너 중심 워크로드 관리를 보완하기 위한 네이티브 바이너리 실행 관리 프레임워크입니다.

- **목적**: 차량 내 로컬 바이너리의 실행 컨텍스트 유지 및 재시작 정책 관리
- **Pullpiri 역할**: 컨테이너 배포 및 노드 레벨 관리
- **본 시스템 역할**: Instance ID 기반 바이너리 프로세스 추적 및 라이프사이클 관리

## 핵심 기능

### 프로세스 관리
- ✅ **시작/중지/상태 조회**: gRPC 인터페이스를 통한 원격 제어
- ✅ **Instance ID 추적**: 고유 인스턴스 식별자 기반 관리 (`inst-{timestamp}-{seq}`)
- ✅ **Service Name (1:N 매핑)**: 하나의 서비스 이름으로 여러 인스턴스 관리 (Kubernetes 패턴)
- ✅ **바이너리 일관성 검증**: 같은 서비스는 같은 바이너리만 허용 (systemd/Kubernetes 표준)
- ✅ **자동 Service Name 생성**: 명시하지 않은 경우 바이너리 이름 기반 자동 생성
- ✅ **Graceful Shutdown**: SIGTERM → 대기(timeout) → SIGKILL 순차 처리
- ✅ **Force Stop**: 즉시 SIGKILL 전송

### 재시작 정책
- ✅ **Never**: 종료 시 재시작하지 않음
- ✅ **OnFailure**: exit code ≠ 0인 경우에만 재시작
- ✅ **Always**: 정상/비정상 종료 모두 재시작
- ✅ **max_retries**: 재시작 최대 횟수 (예: 3이면 최초 1회 + 재시작 3회 = 총 4회 실행)
- ✅ **restart_delay**: 재시작 전 대기 시간 설정

### 모니터링 및 로깅
- ✅ **실시간 프로세스 감시**: 1초 주기 check_and_reap() 실행
- ✅ **이벤트 히스토리**: Started/Stopped/Completed/Crashed/Restarted 이벤트 추적
- ✅ **JSONL 영속화**: 비동기 mpsc 채널 기반 배치 로깅 (10개 단위 flush)
- ✅ **구조화된 로깅**: tracing 프레임워크 통합 (info/warn/error)
- ✅ **프로세스 메트릭**: /proc 기반 상태/메모리 수집
- ✅ **통계 카운터**: 시작/중지/완료/크래시/재시작 집계

### Kyron Runtime 통합
- ✅ **비동기 태스크 스케줄링**: `kyron::spawn()` 활용
- ✅ **타이머 기반 모니터링**: `kyron::futures::sleep::sleep()` 1초 주기
- ✅ **이벤트 루프**: `kyron::runtime::RuntimeBuilder` + `runtime.block_on()`
- ✅ **워커 스레드**: ExecutionEngine 2개 워커 구성

## 아키텍처

```
┌────────────────────────────────────────────────────────┐
│   Pullpiri (ActionController)                         │
│   - Binary artifact 관리                               │
│   - Scenario 실행 트리거                               │
└─────────────────┬──────────────────────────────────────┘
                  │ gRPC :50051
                  ▼
┌────────────────────────────────────────────────────────┐
│   grpc_lifecycle (Kyron Runtime)                      │
│                                                        │
│   ┌──────────────────────────────────────────────┐   │
│   │  lifecycle_server (server_kyron.rs)          │   │
│   │  - BinaryLifecycleService (gRPC impl)        │   │
│   │  - Tokio gRPC Server (port 50051)            │   │
│   └──────────────────────────────────────────────┘   │
│                                                        │
│   ┌──────────────────────────────────────────────┐   │
│   │  BinaryManager (manager.rs)                  │   │
│   │  - instances: HashMap<String, Instance>      │   │
│   │  - check_and_reap(): 프로세스 모니터링       │   │
│   │  - spawn_process(): Child 생성               │   │
│   │  - history: VecDeque<Event> (max 1000)       │   │
│   │  - mpsc 비동기 로깅 채널                     │   │
│   └──────────────────────────────────────────────┘   │
│                                                        │
│   ┌──────────────────────────────────────────────┐   │
│   │  Kyron Runtime                                │   │
│   │  - kyron::runtime::RuntimeBuilder            │   │
│   │  - ExecutionEngine (2 workers)               │   │
│   │  - kyron::spawn(monitor_task)                │   │
│   │  - kyron::futures::sleep (1s timer)          │   │
│   └──────────────────────────────────────────────┘   │
│                                                        │
│   ┌──────────────────────────────────────────────┐   │
│   │  OS Process (std::process::Child)            │   │
│   │  - /bin/sleep, /usr/bin/myapp, ...           │   │
│   │  - SIGTERM / SIGKILL signal 처리             │   │
│   │  - /proc/{pid}/stat, /proc/{pid}/statm       │   │
│   └──────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────┘
```

## 빌드 및 실행

### 빌드

```bash
cd /home/lge/Desktop/2track/orchestrator

# 디버그 빌드
cargo build -p grpc_lifecycle

# 릴리즈 빌드 (권장)
cargo build -p grpc_lifecycle --release
```

### 서버 실행

```bash
# 릴리즈 모드 실행
cargo run -p grpc_lifecycle --bin lifecycle_server --release

# 로그 파일 저장
cargo run -p grpc_lifecycle --bin lifecycle_server --release 2>&1 | tee /tmp/lifecycle.log
```

**네트워크 바인딩**:
- 서버는 기본적으로 `0.0.0.0:50051`에서 gRPC 요청을 수신합니다
- `0.0.0.0`: 모든 네트워크 인터페이스에서 수신 (로컬 + 외부)
- **멀티노드 환경**: 다른 노드에서 해당 노드의 lifecycle_server 접근 가능
- **보안 권장사항**: 프로덕션 환경에서는 방화벽 설정 또는 특정 인터페이스 바인딩 고려

**접속 예시**:
```bash
# 같은 노드에서 (로컬)
http://127.0.0.1:50051

# 다른 노드에서 (멀티노드)
http://192.168.1.100:50051
http://vehicle-ecu-01:50051
```

### 클라이언트 사용 예시

**간편한 사용을 위한 설정** (권장):

```bash
# 1. 릴리즈 빌드 (최초 1회)
cd /home/lge/Desktop/2track/orchestrator
cargo build -p grpc_lifecycle --release

# 2. alias 설정 (선택사항)
alias lifecycle-client='~/Desktop/2track/orchestrator/target/release/lifecycle_client'

# 3. 이제 짧은 명령어로 사용 가능
lifecycle-client start --service nav --binary /bin/sleep --args 30
lifecycle-client status
lifecycle-client stop --service nav
```

또는 바이너리 직접 실행:
```bash
cd /home/lge/Desktop/2track/orchestrator
./target/release/lifecycle_client start --service nav --binary /bin/sleep --args 30
```

**아래 예시는 `cargo run` 방식 (개발 중 편리)**:

#### 1. 바이너리 시작

```bash
# 기본 시작 (인자 없음)
cargo run -p grpc_lifecycle --bin lifecycle_client --release -- \
  start \
  --service test \
  --binary /bin/sleep \
  --args 100

# OnFailure 재시작 정책 (최초 1회 + 재시작 최대 3회 = 총 4회 시도)
# 주의: exit 0으로 정상 종료되면 재시작하지 않음 (실패 시에만 재시작)
# 재시작 테스트를 위해 항상 실패하는 명령어 사용
# /bin/false는 즉시 exit code 1로 종료되어 OnFailure 정책 트리거
cargo run -p grpc_lifecycle --bin lifecycle_client --release -- \
  start \
  --service failing-test \
  --binary /bin/false \
  --policy on_failure \
  --max-retries 3 \
  --delay-secs 1

**참고**: 
- `--service` 생략 시 바이너리 이름 사용 (예: `/bin/sleep` → `sleep`)
- 같은 service name으로 여러 인스턴스 시작 가능 (1:N 매핑)
```

#### 2. 프로세스 중지

```bash
# Service Name으로 중지 (Graceful)
cargo run -p grpc_lifecycle --bin lifecycle_client --release -- \
  stop --service test

# 강제 중지 (즉시 SIGKILL)
cargo run -p grpc_lifecycle --bin lifecycle_client --release -- \
  stop --service test --force

# 전체 프로세스 중지
cargo run -p grpc_lifecycle --bin lifecycle_client --release -- \
  stop --all
```

#### 3. 상태 조회

```bash
# 전체 프로세스 상태
cargo run -p grpc_lifecycle --bin lifecycle_client --release -- status

# Service Name으로 조회 (특정 서비스만 필터링)
# 주의: stats는 항상 전체 시스템 통계를 표시 (특정 서비스만의 통계 아님)
cargo run -p grpc_lifecycle --bin lifecycle_client --release -- \
  status --service test
```

## 설정

### ManagerConfig 커스터마이징

`src/server_kyron.rs`의 `main()` 함수에서 설정 변경:

```rust
let config = manager::ManagerConfig {
    graceful_timeout: Duration::from_secs(10),        // SIGTERM 대기 시간
    max_history_size: 5000,                           // 이벤트 히스토리 최대 크기
    max_spawn_failures: 5,                            // spawn 실패 최대 허용 횟수
    history_log_path: Some(PathBuf::from("/var/log/lifecycle_history.jsonl")),
};
```

### 히스토리 로그 모니터링

```bash
# 실시간 이벤트 확인
tail -f /var/log/lifecycle_history.jsonl

# 특정 서비스 이벤트 필터링
tail -f /var/log/lifecycle_history.jsonl | grep '"service_name":"myapp"'

# JSON 포맷팅
tail -f /var/log/lifecycle_history.jsonl | jq .
```

### Pullpiri 연동 환경 변수

```bash
export ACTIONCONTROLLER_WORKLOAD_RUNTIME=lifecycle
export LIFECYCLE_GRPC_ENDPOINT=http://127.0.0.1:50051
```

## 테스트

### 단위 테스트

```bash
cd /home/lge/Desktop/2track/orchestrator

# 전체 테스트 실행
cargo test -p grpc_lifecycle

# 특정 테스트 실행
cargo test -p grpc_lifecycle restart_policy_always
```

**테스트 목록** (10개 테스트, 모두 통과 ✅):
1. `start_and_stop_by_instance_id` - Instance ID 기반 시작/중지
2. `stop_by_service_name_stops_all` - Service Name으로 여러 인스턴스 중지 (1:N)
3. `multiple_instances_same_binary` - 동일 바이너리 다중 인스턴스 (같은 service_name)
4. `generate_unique_service_name_works` - 1:N 매핑 테스트 (같은 이름 허용)
5. `same_service_different_binary_rejected` - 바이너리 일관성 검증
6. `history_size_limit` - 히스토리 크기 제한
7. `pending_restart_visible_in_status` - 재시작 대기 상태 노출
8. `restart_policy_always_restarts_on_success` - Always 정책 테스트
9. `on_failure_restarts_after_crash` - OnFailure 정책 테스트
10. `max_spawn_failures_stops_retry` - spawn 실패 제한 테스트

### 통합 테스트

Pullpiri와의 연동 테스트:

```bash
# lifecycle_server 실행 (별도 터미널)
cd /home/lge/Desktop/2track/orchestrator
cargo run -p grpc_lifecycle --bin lifecycle_server --release

# 통합 테스트 스크립트 실행
cd /home/lge/Desktop/pullpiri/examples
./lifecycle-test.sh
```

## 주요 기능 상세

### 1. Instance ID 시스템

각 프로세스는 고유한 Instance ID를 부여받습니다:

```
inst-{unix_timestamp}-{sequence}
예: inst-1782348691-1
```

- `unix_timestamp`: 생성 시점 (초 단위)
- `sequence`: 서버 시작 후 증가하는 시퀀스 번호

### 2. Service Name (1:N 매핑)

**하나의 Service Name으로 여러 인스턴스 관리** (Kubernetes/Docker Compose 패턴):

```bash
# 같은 service name으로 3개 인스턴스 시작
$ start --service web --binary /bin/nginx
$ start --service web --binary /bin/nginx
$ start --service web --binary /bin/nginx

# 상태 조회 - 3개 모두 출력
$ status --service web
pid=100 instance=inst-xxx-1 service=web
pid=101 instance=inst-xxx-2 service=web
pid=102 instance=inst-xxx-3 service=web

# 한 번에 모두 중지
$ stop --service web
stopped_count=3
```

**바이너리 일관성 검증** (systemd/Kubernetes 표준):
```bash
# 같은 서비스는 같은 바이너리만 허용
$ start --service web --binary /bin/nginx
success=true

$ start --service web --binary /bin/apache2  # 다른 바이너리!
Error: Service 'web' already uses binary '/bin/nginx' (cannot mix with '/bin/apache2')

$ start --service web --binary /bin/nginx  # 같은 바이너리는 OK
success=true
```

**자동 생성**:
```bash
# --service 생략 시 바이너리 이름 사용
$ start --binary /bin/sleep --args 10
service=sleep  # 여러 번 실행해도 같은 이름
```

### 3. 재시작 정책 동작

#### Never
```
프로세스 종료 → 아무 동작 없음
```

#### OnFailure
```
exit_code == 0 (정상 종료) → 재시작 안함
exit_code != 0 (비정상 종료) → delay 후 재시작 (max_retries 제한)

예시:
- /bin/sleep 5 → exit 0 → 재시작 안함 ✅
- /bin/false → exit 1 → 재시작함 🔄
```

#### Always
```
정상 종료 → delay 후 재시작
비정상 종료 → delay 후 재시작
(max_retries 제한 적용)
```

### 4. 프로세스 모니터링

Kyron 태스크가 1초마다 `check_and_reap()` 실행:

```rust
async fn monitor_task(manager: Arc<Mutex<BinaryManager>>) {
    loop {
        kyron::futures::sleep::sleep(Duration::from_secs(1)).await;
        
        if let Ok(mut mgr) = manager.lock() {
            mgr.check_and_reap();  // 종료된 프로세스 감지 및 처리
        }
    }
}
```

### 5. 로깅 동작

#### 프로세스 시작 시
```
INFO Binary started: instance_id=inst-xxx, pid=12345, service=myapp
```

#### 자연 종료 시 (check_and_reap 감지)
```
WARN Process exited: instance_id=inst-xxx, pid=12345, exit_code=Some(0)
```

#### stop 명령 실행 시
```
WARN Process stopped: instance_id=inst-xxx, service_name=myapp, pid=12345, exit_code=Some(ExitStatus(...)), mode=Graceful
```

### 6. /proc 메트릭 수집

`get_status()` 호출 시 다음 정보 제공:

- **state**: Running, Sleeping, DiskSleep, Stopped, Zombie
- **memory_kb**: RSS 메모리 사용량 (KB)
- **uptime_secs**: 실행 시간 (초)
- **restart_count**: 재시작 횟수

## 문제 해결

### 서버가 시작되지 않음

```bash
# 포트 사용 중인지 확인
ss -tlnp | grep 50051

# 다른 프로세스가 있다면 종료
sudo pkill -f lifecycle_server

# 로그 확인
tail -f /tmp/lifecycle.log
```

### 프로세스가 재시작되지 않음

1. **재시작 정책 확인**
   ```bash
   cargo run -p grpc_lifecycle --bin lifecycle_client -- status --service myapp
   ```
   
2. **max_retries 소진 확인**
   - `restart_count`가 `max_retries`에 도달했는지 확인

3. **spawn 실패 로그 확인**
   ```bash
   grep "failed to spawn" /tmp/lifecycle.log
   ```

4. **OnFailure 정책과 exit code**
   - OnFailure는 exit_code != 0일 때만 재시작
   - 정상 종료(exit 0)는 재시작되지 않음

### 히스토리 로그가 저장되지 않음

1. **디렉토리 권한 확인**
   ```bash
   ls -ld /var/log
   touch /var/log/test.txt  # 쓰기 권한 테스트
   ```

2. **디스크 용량 확인**
   ```bash
   df -h /var
   ```

3. **설정 확인**
   - `history_log_path`가 `Some(...)`로 설정되어 있는지 확인

## gRPC API 스펙

### StartBinary

**Request**:
```protobuf
message StartRequest {
  string service_name = 1;         // 선택 (비어있으면 자동 생성)
  string binary_path = 2;          // 필수
  repeated string args = 3;        // 선택
  RestartPolicy restart_policy = 4;// Never/OnFailure/Always
  uint32 max_retries = 5;          // 재시작 최대 횟수
  uint32 restart_delay_secs = 6;   // 재시작 대기 시간
}
```

**Response**:
```protobuf
message StartResponse {
  bool success = 1;
  uint32 pid = 2;
  string instance_id = 3;
  string service_name = 4;
  string message = 5;
}
```

### StopBinary

**Request**:
```protobuf
message StopRequest {
  uint32 pid = 1;              // PID로 중지
  string instance_id = 2;      // Instance ID로 중지
  string service_name = 3;     // Service Name으로 중지
  bool stop_all = 4;           // 전체 중지
  bool force = 5;              // Force stop (SIGKILL)
  uint32 timeout_secs = 6;     // Graceful timeout (기본 5초)
}
```

**Response**:
```protobuf
message StopResponse {
  bool success = 1;
  uint32 stopped_count = 2;
  string message = 3;
}
```

### GetStatus

**Request**:
```protobuf
message StatusRequest {
  uint32 pid = 1;
  string instance_id = 2;
  string service_name = 3;
  // 모두 비어있으면 전체 조회
}
```

**Response**:
```protobuf
message StatusResponse {
  repeated ProcessInfo processes = 1;
  ManagerStats stats = 2;
}

message ProcessInfo {
  uint32 pid = 1;
  string instance_id = 2;
  string service_name = 3;
  string binary_path = 4;
  string state = 5;              // Running/Sleeping/...
  double uptime_secs = 6;
  uint64 memory_kb = 7;
  uint32 restart_count = 8;
}

message ManagerStats {
  uint64 total_started = 1;
  uint64 total_stopped = 2;
  uint64 total_completed = 3;
  uint64 total_crashed = 4;
  uint64 total_restarted = 5;
}
```

## Pullpiri 연동

### Binary Artifact YAML 예시

```yaml
apiVersion: v1
kind: Binary
metadata:
  name: navigation-service
spec:
  path: /usr/bin/nav_daemon
  args:
    - --config
    - /etc/nav/config.yaml
    - --log-level
    - info
  restartPolicy: OnFailure
  maxRetries: 3
  restartDelaySecs: 5
  node: vehicle-ecu-01
```

### Scenario 예시

```yaml
apiVersion: v1
kind: Scenario
metadata:
  name: nav-launch
spec:
  action: launch
  target: navigation-service
  condition: null  # 즉시 실행
```

자세한 예시는 `/home/lge/Desktop/pullpiri/examples/resources/lifecycle/` 참조.

## 파일 구조

```
grpc_lifecycle/
├── Cargo.toml                    # 패키지 설정
├── build.rs                      # tonic-build proto 컴파일
├── proto/
│   └── lifecycle.proto           # gRPC 서비스 정의
├── src/
│   ├── server_kyron.rs          # lifecycle_server 바이너리 (Kyron 기반)
│   ├── client.rs                # lifecycle_client CLI 도구
│   └── manager.rs               # BinaryManager 핵심 로직
├── docs/
│   └── grpc_lifecycle_architecture.drawio  # 아키텍처 다이어그램
└── README.md                    # 본 문서
```

## 의존성

- **kyron**: Orchestration runtime (비동기 태스크 스케줄링)
- **kyron-foundation**: 공통 유틸리티
- **tonic/prost**: gRPC 서버/클라이언트
- **tokio**: 비동기 런타임 (gRPC용)
- **nix**: POSIX 시그널 처리
- **clap**: CLI 파싱
- **serde/serde_json**: 이벤트 직렬화
- **thiserror**: 에러 타입 정의
- **tracing**: 구조화된 로깅

## 라이선스

Apache-2.0

## 참고 자료

- **아키텍처 다이어그램**: `docs/grpc_lifecycle_architecture.drawio`
- **Pullpiri 통합 테스트**: `/home/lge/Desktop/pullpiri/examples/lifecycle-test.sh`
- **YAML 예시**: `/home/lge/Desktop/pullpiri/examples/resources/lifecycle/`
