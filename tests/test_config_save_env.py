"""A WebUI settings save applies at once, but a Docker-set variable still wins and the reply names it."""

import json
import os
import sys
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
        with urllib.request.urlopen(req, timeout=10) as resp:
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


def test_the_config_reload_keeps_a_docker_set_variable(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "environ", {"CONFIG_DIR": str(tmp_path), "MODE": "report"})
    monkeypatch.setattr(main, "_docker_env_vars", set())
    monkeypatch.setattr(sys, "argv", ["main.py"])
    cfg = main._load_config()
    (tmp_path / "beatscheck.conf").write_text("mode = move\n")
    main._reload_config(cfg)
    assert cfg.saved_mode == "report"
