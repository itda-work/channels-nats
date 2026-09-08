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
└── serializers.py    json 기본, msgpack 선택
tests/                nats-server 바이너리를 띄우는 통합 테스트 (NATS_SERVER, PATH, ~/go/bin 순서로 탐색)
bench/fanout.py       group_send fan-out 지연·처리량. InMemory 레이어와 비교
```

## 규약

- 사용자 쪽 API를 늘리지 않는다. 옵션은 `CONFIG`로만.
- subject 형식(`<prefix>.ch.<channel>`, `<prefix>.grp.<group>`)은 외부 계약이다. 바꾸면 README와 CHANGELOG에 남기고 메이저를 올린다.
- Windows를 1급으로 지원한다. Unix 소켓, fork, 시그널에 의존하지 않는다. CI는 ubuntu와 windows 둘 다.
- 커밋 메시지는 영어 Conventional Commits. 문서는 한국어.

## 함정

- NATS는 저장이 없다. 구독 전 발행은 사라진다. 테스트는 항상 수신자를 먼저 만든다.
- `subscribe` 뒤 `flush()`를 기다려야 서버가 구독을 알고 있다. 이를 빼면 다른 프로세스의 직후 발행을 놓친다.
- 이벤트 루프가 다르면 연결도 다르다. `async_to_sync`가 만든 루프는 별도 연결을 갖는다.
