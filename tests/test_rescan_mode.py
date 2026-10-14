"""A rescan's mode survives the config reload until the saved mode changes after it; a saved setup goes idle."""

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402


def _save(tmp_path, key, value):
    """Set *key* in beatscheck.conf, as a WebUI save or a hand edit would."""
    conf = tmp_path / "beatscheck.conf"
    conf.write_text(re.sub(rf"(?m)^{key} = .*$", f"{key} = {value}", conf.read_text()))


def _cfg(tmp_path, monkeypatch, saved, env=None):
    """Load a config whose beatscheck.conf saves mode = *saved*, with os.environ isolated."""
    monkeypatch.setattr(os, "environ", {"CONFIG_DIR": str(tmp_path), **(env or {})})
    monkeypatch.setattr(main, "_docker_env_vars", set())
    monkeypatch.setattr(sys, "argv", ["main.py"])
    main._write_default_config(str(tmp_path))
    _save(tmp_path, "mode", saved)
    return main._load_config()


def test_rescan_move_survives_the_config_reload(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    main._apply_rescan_trigger("move", cfg)
    main._reload_config(cfg)
    assert cfg.mode == "move"


def test_a_bare_rescan_from_setup_scans_in_report_mode(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "setup")
    monkeypatch.setattr(main, "_idle_wait", lambda *a: "")
    mode = main._run_setup_idle(cfg)
    assert mode == "report"
    main._reload_config(cfg)
    assert cfg.mode == "report"


def test_saving_other_settings_keeps_the_rescan_mode(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    main._apply_rescan_trigger("move", cfg)
    main._reload_config(cfg)
    _save(tmp_path, "workers", "2")
    main._reload_config(cfg)
    assert (cfg.mode, cfg.workers) == ("move", 2)


def test_a_saved_mode_change_replaces_the_rescan_mode(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    main._apply_rescan_trigger("move", cfg)
    main._reload_config(cfg)
    _save(tmp_path, "mode", "setup")
    main._reload_config(cfg)
    assert cfg.mode == "setup"
    main._apply_rescan_trigger("report", cfg)
    main._reload_config(cfg)
    main._reload_config(cfg)
    assert cfg.mode == "report"


def test_without_a_rescan_the_reload_follows_the_saved_mode(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    _save(tmp_path, "mode", "move")
    main._reload_config(cfg)
    assert cfg.mode == "move"


def test_rescan_move_beats_a_docker_mode(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "setup", env={"MODE": "report"})
    assert cfg.mode == "report"
    main._apply_rescan_trigger("move", cfg)
    main._reload_config(cfg)
    assert cfg.mode == "move"


def test_a_mode_saved_just_before_a_rescan_does_not_cancel_it(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    _save(tmp_path, "mode", "setup")
    main._apply_rescan_trigger("move", cfg)
    main._reload_config(cfg)
    assert cfg.mode == "move"


def test_saving_setup_while_running_stops_the_scheduled_scans(tmp_path, monkeypatch):
    music = tmp_path / "music"
    music.mkdir()
    cfg = _cfg(tmp_path, monkeypatch, "report", env={"MUSIC_DIR": str(music)})
    _save(tmp_path, "run_interval", "24")
    modes = []

    def scan(*args, **kwargs):
        modes.append(args[4])
        _save(tmp_path, "mode", "setup")
        if len(modes) > 1:
            monkeypatch.setattr(main, "shutdown_requested", True)

    monkeypatch.setattr(main, "_load_config", lambda: cfg)
    monkeypatch.setattr(main.signal, "signal", lambda *a: None)
    monkeypatch.setattr(os, "nice", lambda n: 0)
    monkeypatch.setattr(main, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(main, "shutdown_requested", False)
    monkeypatch.setattr(main, "run_scan", scan)
    monkeypatch.setattr(main, "_idle_wait", lambda *a: None)
    main.main()
    assert modes == ["report"]
