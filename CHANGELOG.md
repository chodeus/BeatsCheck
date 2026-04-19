# Changelog

## [1.2.0](https://github.com/chodeus/BeatsCheck/compare/v1.1.0...v1.2.0) (2026-04-19)


### Features

* add monotonic BUILD_NUMBER to docker image ([#32](https://github.com/chodeus/BeatsCheck/issues/32)) ([1e075ce](https://github.com/chodeus/BeatsCheck/commit/1e075ceb5639d2fd97421457b145bf16e803a856))
* background file-delete jobs, per-album Lidarr search report, dynamic version ([7ecf7e4](https://github.com/chodeus/BeatsCheck/commit/7ecf7e44d08328781ba62e4fd11a37524034e080))

## [1.1.0](https://github.com/chodeus/BeatsCheck/compare/v1.0.0...v1.1.0) (2026-04-19)


### Features

* **webui:** bulk album delete + Lidarr re-download completion tracking ([#20](https://github.com/chodeus/BeatsCheck/issues/20)) ([2e0c79a](https://github.com/chodeus/BeatsCheck/commit/2e0c79aaae82bd33211d6640a1a519a91afe4c33))

## 1.0.0 (2026-04-19)


### Features

* add favicon to WebUI browser tab ([3c11ada](https://github.com/chodeus/BeatsCheck/commit/3c11adacd41e0df0d70ab89aceed982f4afdb548))
* album view toggle, corrupt page improvements, config subtitle ([c95b6b8](https://github.com/chodeus/BeatsCheck/commit/c95b6b82856ebc545b058fec90dd0269ee7d3e7b))
* collapsible Lidarr info banner on corrupt files page ([1410036](https://github.com/chodeus/BeatsCheck/commit/14100367abb0115d7c88e7866fe8cfc4e46fffd4))
* interactive folder picker for scan and quarantine paths ([205358f](https://github.com/chodeus/BeatsCheck/commit/205358f928ab7d3543e31970350ab48e3e1fc062))
* path dropdown for scan location and quarantine folder ([726e3a3](https://github.com/chodeus/BeatsCheck/commit/726e3a3a1533c66ca63b04033b410787733bcbe9))
* production-ready WebUI with auth, security hardening, and accessibility ([a77c079](https://github.com/chodeus/BeatsCheck/commit/a77c07922f3fd8710d1de50d1b1b51282d8007a1))
* redesign corrupt page and config with workflow-oriented UX ([af47832](https://github.com/chodeus/BeatsCheck/commit/af47832c8f0603ad4fdac54913557fabeb490f08))
* write corrupt_details.json incrementally during scan ([5bb4e56](https://github.com/chodeus/BeatsCheck/commit/5bb4e56d30586260a14edd6056241d4a20c34c4e))


### Bug Fixes

* block deletes during scan, add scan-in-progress banner ([86b0f3a](https://github.com/chodeus/BeatsCheck/commit/86b0f3a6f0c7dc54beb131785de74facc7bc1701))
* cancel scan responsiveness, log coloring, UI improvements ([8f1c5a4](https://github.com/chodeus/BeatsCheck/commit/8f1c5a4be3b786cd6f5111be2bb3c12a0730be69))
* color the status dot icon instead of the status text on dashboard ([#5](https://github.com/chodeus/BeatsCheck/issues/5)) ([3ffa213](https://github.com/chodeus/BeatsCheck/commit/3ffa2133b13f3b04685c8599714f422ea45bff7f))
* config hot-reload, env sync, and Lidarr log safety ([1815eb2](https://github.com/chodeus/BeatsCheck/commit/1815eb2922dbb940a8027094aa9b3c2f7b98e6d2))
* delete processed.txt before writing rescan trigger to prevent race ([9e813c2](https://github.com/chodeus/BeatsCheck/commit/9e813c2ee818b0aafb6da7a5c2e78e952d57c8ef))
* early return on scan cancel to prevent misleading completion log ([1d12552](https://github.com/chodeus/BeatsCheck/commit/1d12552ef95ec58f199953509eaf4d106fe4888b))
* extract _post_scan_wait to reduce main() complexity below lint threshold ([fcd3815](https://github.com/chodeus/BeatsCheck/commit/fcd3815033e72a81ed3c4053f900b5102d5c671e))
* folder picker 403 error and add host path display ([36f5876](https://github.com/chodeus/BeatsCheck/commit/36f58763733a68d29221a56706020567228f66ce))
* graceful path validation instead of crashing container ([7e6c4e6](https://github.com/chodeus/BeatsCheck/commit/7e6c4e6196c7366b167c401fd3351a4300ec2d9e))
* harden Lidarr integration in scan flow ([0d412c0](https://github.com/chodeus/BeatsCheck/commit/0d412c0bd6ed94ed0816d3a9e731760a89f3b5bf))
* harden path containment checks and fix fresh-rescan race ([5a0e232](https://github.com/chodeus/BeatsCheck/commit/5a0e232628ef5a11595479c0e56f1a19d167cc84))
* iOS input zoom and blocklist status in info banner ([ac49b59](https://github.com/chodeus/BeatsCheck/commit/ac49b59d1883df458e3b091471af2b31eac6b115))
* log remaining in-progress files when scan is cancelled ([e8827a1](https://github.com/chodeus/BeatsCheck/commit/e8827a192babc5d8cc8356823f3823194ce5cc6a))
* music_dir not applied on rescan, improve scan UX ([9e3af6e](https://github.com/chodeus/BeatsCheck/commit/9e3af6ef8f58eceef2599b739713d69d8dc34b98))
* remove dead unfiltered Lidarr trackfile bulk fetch ([963986d](https://github.com/chodeus/BeatsCheck/commit/963986d5db07acc9ad4e048aac3eb42f78642832))
* remove MUSIC_DIR/OUTPUT_DIR from Dockerfile ENV ([691a935](https://github.com/chodeus/BeatsCheck/commit/691a935cd5abc33d0964868b967643f4d91439fa))
* remove redundant sys import that failed lint ([0bbe5a2](https://github.com/chodeus/BeatsCheck/commit/0bbe5a268c7521c409c115beeabf27da2abe103c))
* **repo-events:** update chodeus-ops path to .github/workflows/ ([7a391e8](https://github.com/chodeus/BeatsCheck/commit/7a391e84e3810b033858a2fc951d267d6b7c9558))
* resilient Lidarr delete with re-resolve, individual fallback, error reporting ([4a0a8e6](https://github.com/chodeus/BeatsCheck/commit/4a0a8e60d57f1c2a2c8a22f2b00ef12ecc22b28e))
* resolve Lidarr IDs inline during scan instead of blocking at end ([50a42fa](https://github.com/chodeus/BeatsCheck/commit/50a42fae4e74755554435da4804a0596ac69ca1e))
* revert host_data_path feature, keep 403 fix and editable input ([f2cca5f](https://github.com/chodeus/BeatsCheck/commit/f2cca5f22874f2ec49c458a436e6861373cac19c))
* scan order, cancel bug, live library size, status colors ([#4](https://github.com/chodeus/BeatsCheck/issues/4)) ([7d95110](https://github.com/chodeus/BeatsCheck/commit/7d95110e4e59f23623e93f69dd66da383d783a2e))
* select-all in album view, add Re-download Selected, sequential Lidarr deletes ([eb87ce7](https://github.com/chodeus/BeatsCheck/commit/eb87ce7cc4b75902c3a0a25edca15493bb42a7fd))
* set status to idle before finalization on "Nothing to do" path ([#7](https://github.com/chodeus/BeatsCheck/issues/7)) ([1bcd58a](https://github.com/chodeus/BeatsCheck/commit/1bcd58a77e7549a672759002280b3b64b7df15fa))
* skip Lidarr index build when there are no files to scan ([f7fcfb1](https://github.com/chodeus/BeatsCheck/commit/f7fcfb12450ba4ad6cd070c75793f8b4d09d1128))
* sort JSON output and improve Lidarr resolution logging ([b761a9c](https://github.com/chodeus/BeatsCheck/commit/b761a9cc1f3b0cf9ef43d166987d449d90957979))
* suppress CodeQL false positive for path injection in static file serving ([7409f7c](https://github.com/chodeus/BeatsCheck/commit/7409f7c829dffb6c50107356cc7319d97d67f579))
* swallow client-disconnect errors in JSON response writer ([#10](https://github.com/chodeus/BeatsCheck/issues/10)) ([e5e69ba](https://github.com/chodeus/BeatsCheck/commit/e5e69babeb09b71366a44089e3752ce81788a74e))
* update status to idle immediately after scan completes ([#6](https://github.com/chodeus/BeatsCheck/issues/6)) ([7907c5d](https://github.com/chodeus/BeatsCheck/commit/7907c5d8e3ddd9f238ca1067d5e2db4fbbe4cedf))
* use explicit 16px for input font-size to prevent iOS zoom ([d616aaa](https://github.com/chodeus/BeatsCheck/commit/d616aaa156a8b5891b66239671b8c6a6ed25a520))
* WebUI bugs — config labels, missing fields, layout, cancel scan ([1eaa348](https://github.com/chodeus/BeatsCheck/commit/1eaa3485499e1718f432d24e6732dd812e78d3c3))
* write all logger output to log file so WebUI logs match container logs ([65dd7de](https://github.com/chodeus/BeatsCheck/commit/65dd7de9535e5158340c5b8cfb217e4ac63376aa))


### Refactoring

* /music → /data mount, remove /corrupted volume ([bb72619](https://github.com/chodeus/BeatsCheck/commit/bb72619d08789e993b26276f666d5efdb3668327))
* reorganize repo into app/ and scripts/ directories ([dc53f02](https://github.com/chodeus/BeatsCheck/commit/dc53f022a2be5878416c01a40fe45563265102cc))
* simplify config parsing, fix CodeQL alerts, remove dead code ([f7dfc22](https://github.com/chodeus/BeatsCheck/commit/f7dfc22d1e2d310900874a22b7cecd76530092fe))
* simplify scan core, Lidarr resolver, WebUI polling, and auth ([d064fdd](https://github.com/chodeus/BeatsCheck/commit/d064fddb5dc25178a1ff4a0a0122f56add541a4c))


### Documentation

* simplify beatscheck.conf for non-technical users ([64c753b](https://github.com/chodeus/BeatsCheck/commit/64c753b82338476fbc40be3584e1359cb09b675f))
