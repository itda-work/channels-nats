# Changelog

Keep a Changelog 형식. subject 규약이 바뀌면 여기와 README에 남기고 메이저(1.0 전에는 마이너)를 올린다.

## [0.2.0] - 2026-09-08

### Changed

- 프로세스 전용 채널(`specific.<process>!<id>`)은 채널마다 구독하지 않고 프로세스당 구독 하나(`<prefix>.pc.<process>`)로 받는다. 전체 채널 이름은 NATS 헤더 `Channel`로 전달된다. 컨슈머 연결 하나의 비용이 NATS 구독과 콜백 태스크에서 로컬 대기열 하나로 준다 (#1)
- `channel_subject()`가 `!`가 있는 이름에 대해 `pc` subject를 돌려준다. `ch` subject는 일반 채널 전용

## [0.1.0] - 2026-09-08

- 첫 버전. `send`/`receive`/`new_channel`/`group_*`/`flush`, json과 msgpack, 루프별 연결, Ubuntu·Windows CI, fan-out 벤치
