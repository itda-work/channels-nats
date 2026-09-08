# Changelog

Keep a Changelog 형식. subject 규약이 바뀌면 여기와 README에 남기고 메이저(1.0 전에는 마이너)를 올린다.

## [Unreleased]

### Changed

- `close()`도 닫힌 다른 루프의 상태를 함께 정리한다. 이전에는 새 루프가 등록될 때만 스윕이 돌아서 마지막 항목이 남았다
- 이벤트 루프 수가 `loop_state_warn_at`(기본 32)을 넘으면 한 번 경고한다. `close()` 없이 버려진 루프는 살아 있는 루프와 구분할 수 없어 연결이 계속 잡히는데, 이전에는 그것이 조용히 자랐다

## [0.2.2] - 2026-09-09

### Fixed

- `!`가 없는 일반 채널을 여러 프로세스가 읽으면 `send` 하나가 전원에게 복제됐다. 같은 채널 이름으로 워커를 N개 띄우면 모든 워커가 모든 작업을 실행했다. 이제 subject와 같은 이름의 큐 그룹으로 구독해 그중 하나만 받는다 (#2). `.ch.` subject에 합류하는 외부 워커도 같은 큐 그룹을 써야 한다

## [0.2.1] - 2026-09-09

### Added

- `scripts/release_check.py`와 `make release-check`. 태그·CHANGELOG 절·License 분류자가 `pyproject.toml`의 version과 맞는지 본다. release.yml이 태그를 받아 같은 스크립트를 돌리므로 로컬과 CI가 같은 검사를 쓴다
- 3노드 클러스터를 띄우는 `nats_cluster` fixture와 `tests/test_cluster.py`. 노드 간 group/채널 라우팅과, 워커가 붙어 있던 노드를 죽였을 때의 페일오버를 검증한다. 구독 관심사가 route를 타고 다른 노드까지 가는 데 걸리는 시간은 README 클러스터 절에 적었다
- 벤치 결과 JSON과 출력에 무엇을 쟀는지 남긴다(`measured`: 레이어 버전, `git describe --dirty`, Python, nats-server). 파일만 보고 어느 버전의 수치인지 알 수 있고, 파일을 바꿔치기한 A/B는 `-dirty`로 드러난다

### Changed

- mailbox가 가득 차 메시지를 버릴 때 메시지마다 찍던 경고를, 첫 드롭 한 번과 이후 60초당 한 번(`drop_log_interval`)으로 줄인다. 자리가 나면 그동안 버린 개수를 한 줄로 남긴다. 가득 찬 상태는 보통 지속되므로 이전에는 과부하 시 로그가 폭주했다

### Fixed

- nats-py가 재접속을 포기하고 클라이언트를 닫으면(기본 `max_reconnect_attempts=60` 소진) 그 뒤 새로 연 연결에 구독이 하나도 없어서, 프로세스가 발행은 계속하고 수신은 영원히 못 하는 무음 상태가 됐다. 이제 재접속할 때 해당 루프의 프로세스·채널·그룹 구독을 모두 복구한다
- 컨슈머가 끊긴 뒤에도 mailbox가 남아, 메모리가 동시 연결 수가 아니라 누적 연결 수에 비례해 자랐다. 마지막 `receive()`가 취소되면 mailbox와 일반 채널 구독, 남은 그룹 멤버십을 정리한다
- 다른 프로세스가 만든 프로세스 전용 채널로 `receive`/`group_add`를 부르면 그 프로세스의 `pc` subject를 구독해 버려서, 해당 채널 메시지가 양쪽에 중복 전달됐다. 이제 `ValueError`로 막는다. 그 채널로 `send`하는 것은 그대로 동작한다
- `flush()`가 이미 끊긴 연결에서 `ConnectionClosedError`로 터졌다. 구독 해제 실패를 `_discard_mailbox`와 같은 방식으로 흘려보내고, 상태는 첫 await 전에 비운다. NATS가 죽은 상태의 종료가 에러가 되지 않는다
- 이벤트 루프별 상태(`_states`)가 루프가 닫힌 뒤에도 남아 있었다. 새 루프가 등록될 때 닫힌 루프의 상태를 쓸어낸다. 루프를 만들고 버리는 코드에서 죽은 루프와 연결이 쌓이지 않는다
- `channels` 하한을 4.2.1로 올린다. 레이어가 쓰는 `require_valid_channel_name`/`require_valid_group_name`이 4.2.1부터 있어서, 4.0~4.2.0에서는 첫 `send()`가 AttributeError로 죽었다
- `channels_nats.__version__`이 `"0.2.0"`에 멈춰 있었다. 이제 설치 메타데이터에서 읽어 `pyproject.toml`과 어긋날 수 없다
- MIT LICENSE 파일을 추가하고 `license-files`로 sdist·wheel에 싣는다. 0.2.0 배포물에는 라이선스 원문이 없었다
- `License :: OSI Approved :: MIT License` 분류자를 뺀다. PEP 639에서 `License-Expression`과 함께 쓰는 것이 금지됐다. `scripts/release_check.py`가 재발을 막는다 (`twine check`는 잡지 못한다)

## [0.2.0] - 2026-09-08

### Changed

- 프로세스 전용 채널(`specific.<process>!<id>`)은 채널마다 구독하지 않고 프로세스당 구독 하나(`<prefix>.pc.<process>`)로 받는다. 전체 채널 이름은 NATS 헤더 `Channel`로 전달된다. 컨슈머 연결 하나의 비용이 NATS 구독과 콜백 태스크에서 로컬 대기열 하나로 준다 (#1)
- `channel_subject()`가 `!`가 있는 이름에 대해 `pc` subject를 돌려준다. `ch` subject는 일반 채널 전용

## [0.1.0] - 2026-09-08

- 첫 버전. `send`/`receive`/`new_channel`/`group_*`/`flush`, json과 msgpack, 루프별 연결, Ubuntu·Windows CI, fan-out 벤치
