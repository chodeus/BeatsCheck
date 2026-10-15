"""A WebUI settings save applies at once, but a Docker-set variable still wins and the reply names it."""

import json
import os
import sys
import threading
import urllib.error
import urllib.request

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import main  # noqa: E402
import webui  # noqa: E402


@pytest.fixture
def save(webui_server, tmp_path, monkeypatch):
    """POST /api/config with the login off and MODE=report set by Docker; returns the reply."""
    monkeypatch.setattr(os, "environ", {"MODE": "report"})
    monkeypatch.setattr(main, "_docker_env_vars", {"MODE"})
    (tmp_path / "beatscheck.conf").write_text('mode = report\nlidarr_url = ""\n')
    webui._save_auth_off(str(tmp_path))

    def post(config):
        req = urllib.request.Request(
            webui_server + "/api/config", data=json.dumps({"config": config}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:  # nosec B310  # the fixture's http://127.0.0.1 URL
            return json.loads(resp.read())
    return post


def test_a_save_keeps_a_docker_set_variable_and_names_it(save):
    reply = save({"mode": "setup", "lidarr_url": "http://lidarr:8686"})
    assert os.environ["MODE"] == "report"
    assert reply["docker_set"] == ["mode"]


def test_a_save_applies_a_setting_no_docker_variable_sets(save):
    save({"lidarr_url": "http://lidarr:8686"})
    assert os.environ["LIDARR_URL"] == "http://lidarr:8686"


def test_a_docker_set_variable_the_save_agrees_with_is_not_named(save):
    reply = save({"mode": "report"})
    assert reply["docker_set"] == []


def test_a_save_names_only_the_docker_set_keys_it_saved(save, tmp_path):
    (tmp_path / "beatscheck.conf").write_text('mode = setup\nlidarr_url = ""\n')
    reply = save({"lidarr_url": "http://lidarr:8686"})
    assert reply["docker_set"] == []


def test_a_save_it_cannot_read_back_replies_with_an_error(save, monkeypatch):
    def unreadable(path):
        raise OSError("unreadable")
    monkeypatch.setattr(main, "_parse_config_lines", unreadable)
    with pytest.raises(urllib.error.HTTPError) as err:
        save({"lidarr_url": "http://lidarr:8686"})
    assert err.value.code == 500
    assert "LIDARR_URL" not in os.environ


def test_a_save_whose_file_is_gone_before_it_applies_replies_with_an_error(save, monkeypatch):
    write = webui._write_config_file

    def write_then_remove(config_dir, updates):
        ok = write(config_dir, updates)
        os.remove(os.path.join(config_dir, "beatscheck.conf"))
        return ok
    monkeypatch.setattr(webui, "_write_config_file", write_then_remove)
    with pytest.raises(urllib.error.HTTPError) as err:
        save({"lidarr_url": "http://lidarr:8686"})
    assert err.value.code == 500
    assert "LIDARR_URL" not in os.environ


@pytest.mark.parametrize("older", ["save", "reload"])
def test_an_older_read_of_the_file_is_not_applied_after_a_newer_save(save, tmp_path, monkeypatch, older):
    parse, paused, go = main._parse_config_lines, threading.Event(), threading.Event()

    def pause_first_read(path):
        vals = parse(path)
        if not paused.is_set():
            paused.set()
            go.wait(5)
        return vals
    monkeypatch.setattr(main, "_parse_config_lines", pause_first_read)
    if older == "save":
        first = threading.Thread(target=save, args=({"lidarr_url": "http://old"},))
    else:
        first = threading.Thread(target=main._sync_env_from_config_file, args=(str(tmp_path),))
    first.start()
    reached = paused.wait(5)
    second = threading.Thread(target=save, args=({"lidarr_url": "http://new"},))
    second.start()
    second.join(0.5)
    go.set()
    first.join(5)
    second.join(5)
    assert reached
    assert os.environ["LIDARR_URL"] == "http://new"


@pytest.mark.parametrize("paths", [[], ["music", "out", "beats_check.log"]], ids=["docker", "path-arguments"])
def test_the_config_reload_keeps_a_docker_set_variable(tmp_path, monkeypatch, paths):
    monkeypatch.setattr(os, "environ", {"CONFIG_DIR": str(tmp_path), "MODE": "report"})
    monkeypatch.setattr(main, "_docker_env_vars", set())
    monkeypatch.setattr(sys, "argv", ["main.py"] + [str(tmp_path / p) for p in paths])
    cfg = main._load_config()
    (tmp_path / "beatscheck.conf").write_text("mode = move\n")
    main._reload_config(cfg)
    assert cfg.saved_mode == "report"
