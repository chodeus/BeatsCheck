"""A saved mode of delete, or one BeatsCheck doesn't know, leaves a running container in its current mode."""

import json
import os
import re
import sys
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402
import webui  # noqa: E402


def _cfg(tmp_path, monkeypatch, saved):
    """Load a config that saves mode = *saved*, with os.environ isolated."""
    monkeypatch.setattr(os, "environ", {"CONFIG_DIR": str(tmp_path)})
    monkeypatch.setattr(main, "_docker_env_vars", set())
    monkeypatch.setattr(sys, "argv", ["main.py"])
    main._write_default_config(str(tmp_path))
    _save(tmp_path, saved)
    return main._load_config()


def _save(tmp_path, mode):
    conf = tmp_path / "beatscheck.conf"
    conf.write_text(re.sub(r"(?m)^mode = .*$", f"mode = {mode}", conf.read_text()))


def test_a_saved_delete_keeps_the_current_mode_and_says_how_to_delete(tmp_path, monkeypatch, caplog):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    _save(tmp_path, "delete")
    main._reload_config(cfg)
    assert cfg.mode == "report"
    assert "docker exec -it beatscheck delete" in caplog.text


def test_an_unknown_saved_mode_keeps_the_current_mode(tmp_path, monkeypatch, caplog):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    _save(tmp_path, "reprot")
    main._reload_config(cfg)
    assert cfg.mode == "report"
    assert "Unknown mode 'reprot'" in caplog.text


def test_a_saved_mode_in_capitals_still_applies(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    _save(tmp_path, "MOVE")
    main._reload_config(cfg)
    assert cfg.mode == "move"


def test_a_saved_delete_does_not_end_a_rescan(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, monkeypatch, "report")
    main._apply_rescan_trigger("move", cfg)
    main._reload_config(cfg)
    _save(tmp_path, "delete")
    main._reload_config(cfg)
    assert cfg.mode == "move"


def _post_config(base, config):
    req = urllib.request.Request(
        base + "/api/config", data=json.dumps({"config": config}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:  # nosec B310  # the fixture's http://127.0.0.1 URL
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


@pytest.mark.parametrize("mode, status", [("delete", 400), ("reprot", 400), (1, 400), ("Move", 200)])
def test_the_webui_saves_only_a_mode_a_running_container_can_use(webui_server, tmp_path, monkeypatch, mode, status):
    monkeypatch.setattr(os, "environ", {})
    monkeypatch.setattr(main, "_docker_env_vars", set())
    (tmp_path / "beatscheck.conf").write_text("mode = report\n")
    webui._save_auth_off(str(tmp_path))
    got = _post_config(webui_server, {"mode": mode})
    saved = (tmp_path / "beatscheck.conf").read_text()
    assert got == status
    assert ("Move" in saved) == (status == 200)
