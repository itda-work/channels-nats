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
| `capacity` | `100` | 채널당 로컬 대기열 크기. 넘치면 새 메시지를 버리고 경고 로그 |
| `channel_capacity` | `None` | 채널 이름 패턴별 용량 (Channels 규약과 같음) |
| `serializer` | `"json"` | `"json"` 또는 `"msgpack"` |
| `connect_options` | `{}` | `nats.connect()`에 그대로 전달 (재접속, TLS 등) |

## Channels 규약과 다른 점

NATS는 저장 없는 at-most-once pub/sub이다. 이 레이어가 그 위에서 지키는 것과 못 지키는 것.

- **수신자가 먼저 있어야 한다.** 채널의 첫 `receive()`(또는 `new_channel()`) 전에 발행된 메시지는 사라진다. 컨슈머는 연결 시 구독하므로 일반 코드에는 영향이 없고, 임의 이름의 채널에 먼저 `send`하고 나중에 `receive`하는 패턴만 다르다.
- **`ChannelFull`은 발생하지 않는다.** 보내는 쪽은 상대 대기열을 모른다. 대신 받는 쪽이 넘치는 메시지를 버린다.
- **그룹 멤버십은 프로세스 안에 있다.** 프로세스가 죽으면 그 멤버십도 사라지므로 `group_expiry`는 형식상 유지된다.
- **연결은 이벤트 루프마다 하나**다. Django 시그널이나 뷰에서 `async_to_sync(layer.group_send)`를 불러도 된다.

## subject 규약

| subject | 의미 |
|---------|------|
| `<prefix>.ch.<channel>` | `send(channel, message)` |
| `<prefix>.grp.<group>` | `group_send(group, message)`. 그룹에 멤버가 있는 프로세스마다 구독 하나 |

본문은 serializer로 직렬화한 Channels 메시지 dict다. 이 규약만 지키면 Go로 만든 WebSocket 프런트가 Python 없이도 같은 그룹에 뿌릴 수 있다. 그때도 Django 쪽 코드는 바뀌지 않는다.

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

## 벤치마크

`make bench`가 프로세스 P개에 멤버 채널 N개를 나눠 구독시키고 `group_send`를 M회 발행해, 발행에서 각 멤버의 `receive`까지의 지연과 처리량을 잰다. 결과는 `bench/results/`에 남는다. 아래는 macOS arm64, Python 3.12, nats-server 2.14.6에서 잰 값이다 (2026-09-08).

| 구성 | 전달 | p50 | p99 | 처리량 |
|------|-----:|----:|----:|-------:|
| 멤버 1,000, 프로세스 4, 발행 50회 | 50,000 / 50,000 | 0.83 ms | 1.46 ms | 92k/s (발행 간격 10 ms에 묶임) |
| 멤버 10,000, 프로세스 8, 발행 20회 | 200,000 / 200,000 | 4.3 ms | 16.5 ms | 924k/s |

참고로 InMemory 레이어(프로세스 하나)는 멤버 1,000의 `group_send`에 62 ms, 10,000에 10.7초가 걸린다. Python이 멤버 수만큼 돌기 때문이고, NATS에서는 그 일이 Go 서버로 넘어간다.

```bash
make bench ARGS="--members 5000 --processes 8 --messages 50"
```

## 실전 확인

django-wireview의 테스트 프로젝트와 브라우저 E2E가 이 레이어 위에서 통과하며, daphne 4개를 NATS로 묶었을 때 2,000 연결 브로드캐스트가 862 ms에서 221 ms로, 이벤트 처리량이 3,013/s에서 10,485/s로 늘었다. 수치와 재현 명령은 [django-wireview의 설계 문서](https://github.com/itda-work/django-wireview/blob/main/docs/design/transport-abstraction.md)에 있다.

## 개발

```bash
uv sync --all-extras
NATS_SERVER=~/go/bin/nats-server uv run pytest    # PATH에 있으면 환경변수 불필요
uv run ruff check . && uv run pyright
```
