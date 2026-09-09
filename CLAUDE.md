# channels-nats AI Guide

> Django Channels 채널 레이어의 NATS 구현. 사용자 코드는 Channels 표준 그대로이고, 바뀌는 것은 settings의 BACKEND뿐이어야 한다.

## 정본

| 무엇 | 정본 |
|------|------|
| 의존성, 지원 Python | `pyproject.toml` |
| 레이어 의미론과 subject 규약 | `README.md`, `channels_nats/layer.py` 모듈 docstring |
| Channels 레이어 스펙 | `channels.layers.BaseChannelLayer` (send/receive/new_channel/group_*/flush) |

## 지도

```
channels_nats/
├── __init__.py       NatsChannelLayer 재export
├── layer.py          레이어 본체. 이벤트 루프별 연결, 채널별 로컬 mailbox, 그룹당 구독 하나
└── serializers.py    msgpack 고정. 형식은 프로세스 간 계약이라 옵션이 아니다
tests/                nats-server 바이너리를 띄우는 통합 테스트 (NATS_SERVER, PATH, ~/go/bin 순서로 탐색)
bench/fanout.py       group_send fan-out 지연·처리량. InMemory 레이어와 비교
scripts/release_check.py  태그·CHANGELOG·분류자가 pyproject의 version과 맞는지
```

## 규약

- 사용자 쪽 API를 늘리지 않는다. 옵션은 `CONFIG`로만.
- subject 형식(`<prefix>.ch.<channel>`, `<prefix>.grp.<group>`)은 외부 계약이다. 바꾸면 README와 CHANGELOG에 남기고 메이저를 올린다.
- Windows를 1급으로 지원한다. Unix 소켓, fork, 시그널에 의존하지 않는다. 다만 CI는 ubuntu만 돈다. Windows는 코드 원칙으로 지키는 것이지 CI로 검증되지 않으므로, 타이밍이나 경로에 민감한 변경은 손으로 확인한다(0.2.1의 클러스터 레이스는 Windows에서만 났다).
- 커밋 메시지는 영어 Conventional Commits. 문서는 한국어.
- 릴리스는 `pyproject.toml`의 version을 올리고 CHANGELOG에 절을 추가한 뒤 `make release-check`로 확인하고 `v<version>` 태그를 푸시한다. 버전을 선언하는 곳은 `pyproject.toml` 하나다(`__version__`은 설치 메타데이터에서 읽는다). `release.yml`이 빌드해 PyPI(trusted publishing, environment `pypi`)와 GitHub Release에 올린다. 토큰은 저장하지 않는다.

## 함정

- NATS는 저장이 없다. 구독 전 발행은 사라진다. 테스트는 항상 수신자를 먼저 만든다.
- `subscribe` 뒤 `flush()`를 기다려야 서버가 구독을 알고 있다. 이를 빼면 다른 프로세스의 직후 발행을 놓친다. 클러스터에서는 `flush()`도 자기 노드까지만 보장하고 다른 노드로의 전파는 비동기다. 노드를 넘는 테스트는 도착할 때까지 발행을 반복한다(`tests/test_cluster.py`).
- 직접 `send`(`pc`/`ch`)와 `group_send`(`grp`)는 subject가 달라 서로 간 순서 보장이 없다. 각 경로 안에서만 순서가 산다. 합치려면 발행자에서 그룹을 펼쳐야 하는데 그건 이 레이어가 피하려는 비용이다.
- 이벤트 루프가 다르면 연결도 다르다. `async_to_sync`가 만든 루프는 별도 연결을 갖는다. 닫힌 루프의 상태는 다음 루프 등록이나 `close()`에서 정리되지만, `close()` 없이 버려진 루프는 살아 있는 루프와 구분할 수 없어 연결이 남는다. 그 수가 `loop_state_warn_at`을 넘으면 경고한다.
