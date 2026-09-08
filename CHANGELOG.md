# Changelog

Keep a Changelog 형식. subject 규약이 바뀌면 여기와 README에 남기고 메이저(1.0 전에는 마이너)를 올린다.

## [Unreleased]

### Fixed

- `channels` 하한을 4.2.1로 올린다. 레이어가 쓰는 `require_valid_channel_name`/`require_valid_group_name`이 4.2.1부터 있어서, 4.0~4.2.0에서는 첫 `send()`가 AttributeError로 죽었다
- MIT LICENSE 파일을 추가하고 `license-files`로 sdist·wheel에 싣는다. 0.2.0 배포물에는 라이선스 원문이 없었다
- `License :: OSI Approved :: MIT License` 분류자를 뺀다. PEP 639에서 `License-Expression`과 함께 쓰는 것이 금지됐다. release.yml에 재발 방지 검사를 넣었다 (`twine check`는 잡지 못한다)

## [0.2.0] - 2026-09-08

### Changed

- 프로세스 전용 채널(`specific.<process>!<id>`)은 채널마다 구독하지 않고 프로세스당 구독 하나(`<prefix>.pc.<process>`)로 받는다. 전체 채널 이름은 NATS 헤더 `Channel`로 전달된다. 컨슈머 연결 하나의 비용이 NATS 구독과 콜백 태스크에서 로컬 대기열 하나로 준다 (#1)
- `channel_subject()`가 `!`가 있는 이름에 대해 `pc` subject를 돌려준다. `ch` subject는 일반 채널 전용

## [0.1.0] - 2026-09-08

- 첫 버전. `send`/`receive`/`new_channel`/`group_*`/`flush`, json과 msgpack, 루프별 연결, Ubuntu·Windows CI, fan-out 벤치
