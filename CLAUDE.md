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
- **작업 큐는 GitHub 이슈다.** 세션을 시작하면 `gh issue list`부터 본다. 결함을 발견하면 그 자리에서 고치거나 이슈로 남긴다 — 요약 말미의 "남은 것" 목록으로 넘기지 않는다. 이슈에는 재현 방법과 **검증 상태**(재현함 / 코드상 확인 / 미검증)를 적고, 심각도는 보수적으로 적는다.
- **결함은 재현한 뒤에 말한다.** 코드를 읽고 추론한 것과 실제로 돌려 본 것을 구분해서 쓴다. 인터페이스 표면 비교(메서드가 다 있는지)로 "누락 없음"이라고 결론 내지 않는다 — 이 레이어의 실제 결함은 전부 동작에 있었다.
- **수정은 "수정 전에 실패하는 테스트"로 증명한다.** `git stash push -q channels_nats/` → 해당 테스트 실행 → `git stash pop -q`. 통과하는 테스트가 잘못된 동작을 고정하고 있던 사례가 있었다(취소 시 그룹 멤버십 삭제).
- **"불가능"이라고 쓰기 전에 근거 범위를 밝힌다.** "이 설계로는 불가능", "이 라이브러리의 공개 API로는 불가능", "유지 비용 때문에 안 한다"는 서로 다른 말이다. 순서 역전을 첫 번째로 단정했다가 정정한 적이 있다(#6).
- 릴리스는 `pyproject.toml`의 version을 올리고 CHANGELOG에 절을 추가한 뒤 `make release-check`로 확인하고 `v<version>` 태그를 푸시한다. 버전을 선언하는 곳은 `pyproject.toml` 하나다(`__version__`은 설치 메타데이터에서 읽는다). `release.yml`이 빌드해 PyPI(trusted publishing, environment `pypi`)와 GitHub Release에 올린다. 토큰은 저장하지 않는다.

## 함정

- NATS는 저장이 없다. 구독 전 발행은 사라진다. 테스트는 항상 수신자를 먼저 만든다.
- `subscribe` 뒤 `flush()`를 기다려야 서버가 구독을 알고 있다. 이를 빼면 다른 프로세스의 직후 발행을 놓친다. 클러스터에서는 `flush()`도 자기 노드까지만 보장하고 다른 노드로의 전파는 비동기다. 노드를 넘는 테스트는 도착할 때까지 발행을 반복한다(`tests/test_cluster.py`). 그래서 그 테스트들이 확인하는 것은 **eventual reachability**이지 최초 1회 발행의 무손실이 아니다 — 전파 창·노드 장애·재접속 중의 유실은 허용된 동작이다. 통과를 "유실 없음"으로 읽지 않는다.
- **취소가 nats-py 안에서 사라질 수 있다.** `_flush_pending()`은 `except asyncio.CancelledError: pass`다. 송신 backpressure로 그 지점에 머무는 동안 취소가 오면 삼켜지고, 태스크는 취소되지 않고 **정상 완료된다**(실제 서버로 확인). 레이어 상태는 일관된다 — mailbox는 만들어지고 구독도 붙는다. 잃는 것은 자원이 아니라 취소 신호다. **감지 가능 여부는 경로마다 다르다.** `group_send` → `publish`의 강제 flush에서 삼켜지면 호출자 태스크의 `Task.cancelling()`이 0 → 1로 남아 3.11+에서는 알 수 있다(실측, nats-py 기본 pending 한도에서도 재현). `_mailbox`의 subscribe 경로는 #19가 0으로 읽었다고 적었지만 **다시 세우지 못해 이번에는 확인하지 못했다**. 두 경로를 묶어 "감지할 수 없다"고 일반화하지 말 것(#19, #23). 정상 disconnect 경로라면 daphne가 1초마다 다시 취소하므로 지연은 유계다. 종료 경로는 다르다: `kill_all_applications`는 한 번만 취소하고 gather한다(#23).
- **취소 재현은 취소 직전에 태스크가 정말 그 지점에 서 있는지 확인해야 한다.** `done()`이 False인지, `cancel()`이 True를 돌려주는지, await 체인이 예상한 곳인지 — 셋 다 본다. 안 보면 이미 끝난 태스크의 정상 완료를 "삼켜진 취소"로 읽는다. 실제로 그렇게 오탐했고 외부 검토에서 잡혔다(#23).
- 순서는 **구독 하나 안에서만** 보장된다. 직접 `send` 대 `group_send`도, 서로 다른 두 그룹도 역전된다. 와이어 순서는 옳고 뒤집는 것은 nats-py의 구독별 디스패치다. 고치려면 수신 측 단일 디스패처가 필요하다 — 불가능한 게 아니라 유지 비용 문제다.
- `uv run`은 매번 프로젝트를 재동기화한다. 반면 venv의 python을 직접 부르면 옛 설치가 남아 `__version__`이 뒤처져 보인다. 배포본 검증은 별도 venv에 `--no-cache --reinstall`로 버전을 못 박아 설치하고, 저장소 밖 디렉터리에서 돌린다(저장소 안에서 돌리면 소스 트리가 설치본을 가린다).
- 배포 직후 PyPI JSON API는 한동안 옛 버전을 준다. 업로드 로그의 200 OK로 먼저 확인하고, 반영은 폴링해서 본다.
- 이벤트 루프가 다르면 연결도 다르다. `async_to_sync`가 만든 루프는 별도 연결을 갖는다. 닫힌 루프의 상태는 다음 루프 등록이나 `close()`에서 정리되지만, `close()` 없이 버려진 루프는 살아 있는 루프와 구분할 수 없어 연결이 남는다. 그 수가 `loop_state_warn_at`을 넘으면 경고한다.
