# channels-nats

> Django Channels 채널 레이어를 NATS 위에 올린다. Redis 대신 Go 단일 바이너리 하나. Windows, macOS, Linux 모두 네이티브.

```python
# settings.py — 바뀌는 것은 이 블록뿐이다
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_nats.NatsChannelLayer",
        "CONFIG": {"servers": ["nats://127.0.0.1:4222"]},
    }
}
```

컨슈머, `group_send`, `get_channel_layer()` 등 Channels 코드는 그대로다. wireview처럼 채널 레이어 위에 올라간 라이브러리도 그대로다.

## 왜

- **Windows에서 WSL2·Docker 없이** 여러 Python 프로세스가 한 레이어를 공유한다. `nats-server.exe`를 PATH에 두면 끝.
- **`group_send`가 발행 한 번**이다. channels_redis는 그룹 멤버 수만큼 명령을 보내지만, NATS는 서버(Go)가 뿌린다.
- **subject가 계약**이라 Python 워커든 Go 프런트든 같은 레이어에 합류할 수 있다 (아래 규약).

## 플랫폼

| | NATS (`nats-server`) | Valkey / Redis |
|---|---|---|
| Linux | 네이티브 바이너리 | 네이티브 |
| macOS | 네이티브, `brew install nats-server` | 네이티브, `brew install valkey` |
| Windows | 네이티브 `.exe` ([릴리스 zip](https://github.com/nats-io/nats-server/releases)) | 공식 빌드 없음. WSL2·Docker, 또는 Memurai·Garnet 같은 호환 서버 |

Go 툴체인이 있으면 어느 OS에서든 `go install github.com/nats-io/nats-server/v2@latest`로 빌드된다.

## 설치

```bash
pip install channels-nats            # 또는 uv add channels-nats
pip install "channels-nats[msgpack]" # bytes를 실어 보내야 하면
```

서버는 `nats-server -p 4222`로 띄운다. 인증이 필요하면 `nats-server --auth <token>`과 `CONFIG: {"servers": ["nats://<token>@host:4222"]}`.

## 설정

| 키 | 기본값 | 의미 |
|----|--------|------|
| `servers` | `"nats://127.0.0.1:4222"` | 문자열 또는 목록 |
| `prefix` | `"channels"` | subject 접두어. 한 NATS를 여러 앱이 나눠 쓸 때 구분 |
| `expiry` | `60` | 초. 이보다 오래 대기한 메시지는 `receive`가 버린다 |
| `capacity` | `100` | 채널당 로컬 대기열 크기. 넘치면 새 메시지를 버린다. 경고는 첫 드롭에 한 번, 이후 60초에 한 번, 대기열에 자리가 나면 누적 개수와 함께 한 번 |
| `channel_capacity` | `None` | 채널 이름 패턴별 용량 (Channels 규약과 같음) |
| `serializer` | `"json"` | `"json"` 또는 `"msgpack"` |
| `connect_options` | `{}` | `nats.connect()`에 그대로 전달 (재접속, TLS 등) |

## Channels 규약과 다른 점

NATS는 저장 없는 at-most-once pub/sub이다. 이 레이어가 그 위에서 지키는 것과 못 지키는 것.

- **수신자가 먼저 있어야 한다.** 채널의 첫 `receive()`(또는 `new_channel()`) 전에 발행된 메시지는 사라진다. 컨슈머는 연결 시 구독하므로 일반 코드에는 영향이 없고, 임의 이름의 채널에 먼저 `send`하고 나중에 `receive`하는 패턴만 다르다.
- **`ChannelFull`은 발생하지 않는다.** 보내는 쪽은 상대 대기열을 모른다. 대신 받는 쪽이 넘치는 메시지를 버린다. `channels_redis`는 큐가 `capacity`를 넘으면 보내는 쪽에 이 예외를 던지므로, 그것을 잡던 코드는 여기서 아무 신호도 받지 못한다.
- **버퍼가 있는 곳이 다르다.** `channels_redis`는 메시지를 Redis 안에 `expiry`(기본 60초)까지 보관하므로 받는 쪽이 아직 없어도 나중에 받는다. 이 레이어의 버퍼는 **받는 프로세스의 로컬 mailbox**이고 구독이 생긴 뒤에만 존재한다. 즉 "버퍼가 없다"가 아니라 "버퍼가 브로커가 아니라 구독자 안에 있다"가 정확하다.
- **mailbox는 마지막 `receive()`가 취소될 때 사라진다.** 컨슈머가 끊기면 대기 중이던 `receive()`가 취소되고, 그것이 채널의 끝을 알 수 있는 유일한 신호다. 그때 mailbox와 (일반 채널이면) 그 구독을 정리한다.
- **그룹 멤버십은 프로세스 안에 있다.** 프로세스가 죽으면 그 멤버십도 사라지므로 `group_expiry`는 형식상 유지된다. 다른 프로세스가 만든 채널로 `receive`나 `group_add`를 부르면 `ValueError`다. 그 채널로 `send`하는 것은 물론 된다.
- **연결은 이벤트 루프마다 하나**다. Django 시그널이나 뷰에서 `async_to_sync(layer.group_send)`를 불러도 된다.

## Core NATS만 쓴다 — JetStream을 쓰지 않는 이유

이 레이어는 Core NATS의 `publish`/`subscribe`만 쓴다. JetStream(스트림, 소비자, `.blk` 파일)을 만들지도 열지도 않고, 의존은 `nats-py` 하나다. 디스크에 아무것도 쓰지 않는다.

**Channels 채널 레이어에 JetStream이 필요 없기 때문이다.** Channels 스펙이 요구하는 전달 보장은 at-most-once이고, 그것은 Core NATS가 이미 주는 것이다. 영속 스트림을 얹으면 레이어가 보장하지 않는 것을 보장하는 것처럼 보이게 만들면서 운영 부담(스토리지, 보존 정책, 소비자 상태)만 늘어난다.

**Jepsen의 NATS 보고서는 이 레이어에 해당하지 않는다.** 2025-12 [Jepsen: NATS 2.12.1](https://jepsen.io/analyses/nats-2.12.1)이 `.blk` 파일의 단일 비트 오류로 승인된 쓰기 1,367,069건 중 679,153건(49.7%)이 사라지는 것을 보고했다. 이 레이어를 쓸지 판단할 때 자주 인용될 문서라 범위를 적어 둔다.

- **검증 대상은 JetStream뿐이다.** 보고서가 "We tested NATS JetStream"이라 밝히고, Core NATS는 "Regular NATS streams are allowed to drop messages"라며 범위에서 제외했다.
- **결함은 전부 디스크 영속성 계층의 것이다.** `.blk` 파일 손상(#7549), 스냅샷 파일 손상(#7556), 지연된 fsync(#7564), split-brain(#7567). 프로세스 크래시로 스트림이 통째로 사라지던 #6888은 2.10.23에서 고쳐졌다.
- **이 레이어에는 스트림도 `.blk` 파일도 없다.** 따라서 위 결함이 재현될 표면이 없다.
- **다만 "in-memory는 장애에 강하다"도 이 보고서의 결론이 아니다.** Jepsen은 memory storage 스트림도 Core NATS도 시험하지 않았다. 시험되지 않은 것은 안전이 입증된 것이 아니다. 이 레이어에 기대야 할 보장은 위 "Channels 규약과 다른 점"에 적힌 것, 그것뿐이다.

나중에 영속성이 필요해지면(재연결 중 놓친 메시지 재생 같은) 그때는 이 보고서가 정면으로 해당한다. 그런 기능은 Channels 레이어의 계약 밖이므로 여기가 아니라 애플리케이션에서 다룰 일이다.

## subject 규약

| subject | 의미 |
|---------|------|
| `<prefix>.pc.<process>` | 프로세스 전용 채널 `specific.<process>!<id>`로의 `send`. 전체 채널 이름은 NATS 헤더 `Channel`에 실리고, 받은 프로세스가 로컬에서 라우팅한다. 프로세스당 구독 하나 |
| `<prefix>.ch.<channel>` | `!`가 없는 일반 채널로의 `send`. 채널당 구독 하나이고, subject와 같은 이름의 **큐 그룹**으로 구독한다. 그래서 여러 프로세스가 같은 채널 이름을 읽어도 메시지는 그중 하나에만 간다 |
| `<prefix>.grp.<group>` | `group_send(group, message)`. 그룹에 멤버가 있는 프로세스마다 구독 하나 |

일반 채널의 큐 그룹도 계약의 일부다. `.ch.` subject에 합류하는 외부 워커가 큐 그룹 없이 그냥 구독하면 그 워커도 사본을 받아 단일 전달이 깨진다.

본문은 serializer로 직렬화한 Channels 메시지 dict다. 컨슈머 연결 하나의 비용은 로컬 대기열 하나이고 NATS 구독이 아니다. 이 규약만 지키면 Go로 만든 WebSocket 프런트가 Python 없이도 같은 그룹에 뿌릴 수 있다. 그때도 Django 쪽 코드는 바뀌지 않는다.

## Windows 운영

`nats-server.exe` 하나가 전부다. Windows 서비스로 올리는 방법.

1. [릴리스 zip](https://github.com/nats-io/nats-server/releases)을 `C:\nats\`에 푼다.
2. 설정 파일 `C:\nats\nats.conf`를 만든다. 외부에 열지 않고 토큰을 요구하는 최소 구성이다.

   ```
   listen: 127.0.0.1:4222
   authorization { token: "긴-무작위-문자열" }
   log_file: "C:\nats\nats.log"
   ```

3. 서비스로 등록하고 시작한다 (관리자 PowerShell). nats-server는 Windows 서비스 제어를 직접 지원한다.

   ```powershell
   sc.exe create nats-server binPath= "C:\nats\nats-server.exe -c C:\nats\nats.conf" start= auto
   sc.exe start nats-server
   ```

4. Django 쪽은 URL에 토큰을 넣는다.

   ```python
   CHANNEL_LAYERS = {
       "default": {
           "BACKEND": "channels_nats.NatsChannelLayer",
           "CONFIG": {"servers": [f"nats://{os.environ['NATS_TOKEN']}@127.0.0.1:4222"]},
       }
   }
   ```

여러 Python 프로세스(daphne 등)는 같은 URL로 붙으면 한 레이어를 공유한다. SQLite를 쓰는 단일 서버라면 이것으로 멀티프로세스 구성이 끝난다. 상태 확인은 `sc.exe query nats-server`, 로그는 `nats.log`, 재시작은 `sc.exe stop`과 `start`다.

macOS와 Linux에서는 `brew services start nats-server` 또는 systemd 유닛에 같은 설정 파일을 쓴다.

## 클러스터로 확장

한 대로 부족해지면 NATS 서버를 늘린다. 이 레이어 쪽 코드는 바뀌지 않고, `servers`에 주소를 더 적는 것이 전부다.

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_nats.NatsChannelLayer",
        "CONFIG": {
            "servers": ["nats://n1:4222", "nats://n2:4222", "nats://n3:4222"],
        },
    }
}
```

**Core NATS 클러스터는 무공유다.** 서버끼리 주고받는 것은 "어느 노드에 어떤 subject의 구독자가 있는가"뿐이고, 저장도 합의(Raft)도 없다. 그래서 노드를 넣고 빼는 데 리밸런싱이 없다. 앱 프로세스는 아무 노드에나 붙으면 되고, `<prefix>.grp.<group>`에 발행한 메시지는 그 그룹의 구독자가 있는 노드로만 서버가 전달한다. Redis Cluster처럼 슬롯을 나누거나 마스터·레플리카를 관리할 것이 없다.

**페일오버는 클라이언트가 한다.** 접속하면 서버가 클러스터의 다른 노드 주소를 알려주고(gossip), `nats-py`가 그 목록으로 재접속한다. 그래서 `servers`에는 노드 전부를 적지 않아도 되지만, 첫 접속이 실패하지 않도록 두세 개는 적어 두는 편이 낫다. 재시도 간격 같은 것은 `connect_options`로 `nats.connect()`에 그대로 넘긴다.

```python
"CONFIG": {
    "servers": ["nats://n1:4222", "nats://n2:4222", "nats://n3:4222"],
    "connect_options": {"max_reconnect_attempts": -1, "reconnect_time_wait": 0.5},
}
```

nats-py가 재접속을 포기하고 연결을 닫아 버리면(기본 `max_reconnect_attempts=60`) 레이어가 다음 호출에서 새로 접속하면서 그 이벤트 루프의 구독을 전부 다시 만든다. 0.2.0에서는 이 경우 발행만 되고 수신이 영원히 멈췄다.

**구독이 다른 노드에 알려지는 데는 시간이 걸린다.** `subscribe` 뒤의 `flush()`는 붙어 있는 노드가 그 구독을 안다는 보장일 뿐이다. 관심사가 route를 타고 다른 노드까지 가는 것은 비동기라, 방금 구독한 채널로 **다른 노드에서 곧바로** 발행하면 그 메시지는 사라질 수 있다. 컨슈머는 연결할 때 구독하고 그 뒤로 계속 살아 있으므로 일반 코드에는 영향이 없다. 구독 직후 한 번만 발행하고 도착을 단정하는 코드에서만 문제가 되고, `tests/test_cluster.py`는 도착할 때까지 발행을 반복해서 이 창을 피한다.

**클러스터를 붙여도 저장이 생기지는 않는다.** 위 "Channels 규약과 다른 점"은 그대로다. 재접속이 끝나기 전에 발행된 메시지는 그 프로세스에 도착하지 않고, 노드가 죽으면 그 노드에 붙어 있던 프로세스의 구독은 재접속 후에 다시 만들어진다. 그 사이의 메시지는 사라진다. 노드를 늘려서 얻는 것은 처리량과 가용성이지 전달 보장이 아니다.

리전이 여러 개면 클러스터끼리 gateway로 묶고(supercluster), 사설망·엣지 프로세스는 leaf node로 상위 클러스터에 붙인다. 둘 다 subject 규약과 무관하므로 레이어 설정은 역시 `servers`뿐이다.

### 3노드 예시

서버 쪽 설정만 보면 이렇다. `cluster.routes`로 서로를 가리키고, 클라이언트 포트(4222)와 클러스터 포트(6222)를 나눈다.

```
# n1.conf — n2, n3는 server_name만 바꾼다
server_name: n1
listen: 0.0.0.0:4222
cluster {
  name: channels
  listen: 0.0.0.0:6222
  routes: ["nats://n1:6222", "nats://n2:6222", "nats://n3:6222"]
}
```

docker compose로 띄운다면:

```yaml
services:
  n1: &node
    image: nats:2.14-alpine
    command: >
      --name n1 --cluster_name channels
      --cluster nats://0.0.0.0:6222
      --routes nats://n1:6222,nats://n2:6222,nats://n3:6222
      --http_port 8222
    ports: ["4222:4222", "8222:8222"]
  n2:
    <<: *node
    command: >
      --name n2 --cluster_name channels
      --cluster nats://0.0.0.0:6222
      --routes nats://n1:6222,nats://n2:6222,nats://n3:6222
    ports: ["4223:4222"]
  n3:
    <<: *node
    command: >
      --name n3 --cluster_name channels
      --cluster nats://0.0.0.0:6222
      --routes nats://n1:6222,nats://n2:6222,nats://n3:6222
    ports: ["4224:4222"]
```

`curl localhost:8222/routez`로 라우트가 맺혔는지 확인한다. `tests/test_cluster.py`가 이 구성을 실제로 띄워 노드 간 라우팅과 노드 하나를 죽였을 때의 페일오버를 검증한다. Windows에서 도커 없이 확인하려면 같은 머신에 포트만 달리해 `nats-server.exe -c n1.conf` 셋을 띄우면 된다.

인증을 쓰는 클러스터라면 클라이언트 토큰과 별개로 라우트에도 자격이 필요하다. `cluster { authorization { user: route, password: ... } }`를 세 노드에 같이 넣고 `routes`를 `nats://route:...@n1:6222` 형태로 적는다.

## 벤치마크

`make bench`가 프로세스 P개에 멤버 채널 N개를 나눠 구독시키고 `group_send`를 M회 발행해, 발행에서 각 멤버의 `receive`까지의 지연과 처리량을 잰다. 결과는 `bench/results/`에 남는다. 아래는 macOS arm64, Python 3.12, nats-server 2.14.6에서 잰 값이다 (2026-09-09, 0.2.1).

| 구성 | 전달 | p50 | p99 | 처리량 |
|------|-----:|----:|----:|-------:|
| 멤버 1,000, 프로세스 4, 발행 50회 | 50,000 / 50,000 | 0.64 ms | 1.31 ms | 93k/s (발행 간격 10 ms에 묶임) |
| 멤버 10,000, 프로세스 8, 발행 20회 | 200,000 / 200,000 | 3.2 ms | 5.4 ms | 934k/s |

참고로 InMemory 레이어(프로세스 하나)는 멤버 1,000의 `group_send`에 67 ms, 10,000에 8.7초가 걸린다. Python이 멤버 수만큼 돌기 때문이고, NATS에서는 그 일이 Go 서버로 넘어간다.

같은 기계에서 1,000 멤버 설정을 세 번 돌리면 p50이 0.59~0.75 ms, p99가 1.31~1.76 ms로 흔들린다. 표의 값은 그 중간 회차다.

10,000 멤버의 p99가 0.2.0 표의 16.5 ms에서 5.4 ms로 떨어졌지만 코드 덕이 아니다. `v0.2.0` 태그의 레이어를 오늘 같은 기계에서 돌리면 p50 3.03·3.46 ms, p99 4.97·5.41 ms로 0.2.1(p50 3.24~3.30, p99 5.41~5.46)과 구분되지 않는다. 두 버전은 전달 경로가 같고, 옛 표의 16.5 ms는 그날의 기계 상태였다. 벤치 수치를 버전 간 비교로 읽을 때 주의할 점이다.

```bash
make bench ARGS="--members 5000 --processes 8 --messages 50"
```

## 실전 확인

django-wireview의 테스트 프로젝트와 브라우저 E2E가 이 레이어 위에서 통과한다. daphne 4개를 NATS로 묶은 2,000 연결 실측(항목 5개 컴포넌트)은 다음과 같고, InMemory 레이어의 daphne 1개는 브로드캐스트 862 ms, 이벤트 3,013/s, 연결당 46 KB였다.

| channels-nats | 연결당 RSS | join/s | 이벤트/s | 브로드캐스트 |
|---|---:|---:|---:|---:|
| 0.1.0 채널당 구독 | 61.3 KB | 1,861 | 11,621 | 202 ms |
| 0.2.0 프로세스당 구독 | 55.3 KB | 2,167 | 11,758 | 143 ms |

결과 JSON은 `bench/results/wireview-nats-4proc-0.2.0.json`이다. 수치와 재현 명령은 [django-wireview의 설계 문서](https://github.com/itda-work/django-wireview/blob/main/docs/design/transport-abstraction.md)에 있다.

## 개발

```bash
uv sync --all-extras
NATS_SERVER=~/go/bin/nats-server uv run pytest    # PATH에 있으면 환경변수 불필요
uv run ruff check . && uv run pyright
```
