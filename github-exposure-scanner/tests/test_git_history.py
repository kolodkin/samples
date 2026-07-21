import subprocess

from github_exposure_scanner import git_history as gh

from gitutil import make_repo


def test_clone_mirror_and_repo_dir_fixture(tmp_path, monkeypatch):
    src = str(tmp_path / "src")
    make_repo(src, [{"message": "init", "date": "2020-01-01",
                     "files": {"a.txt": "hello\n"}}])

    # clone_mirror produces a usable object DB
    dest = str(tmp_path / "mirror")
    gh.clone_mirror(src, dest)
    out = subprocess.run(["git", "-C", dest, "rev-list", "--all"],
                         check=True, capture_output=True).stdout
    assert out.strip(), "mirror clone should contain commits"

    # fixture mode: repo_dir_for returns <fixture>/<org>/<repo>, not a temp dir
    fixture_root = str(tmp_path / "fx")
    make_repo(f"{fixture_root}/acme/widgets",
              [{"message": "init", "files": {"a.txt": "hi\n"}}])
    monkeypatch.setenv(gh.GIT_FIXTURE_ENV, fixture_root)
    path, is_temp = gh.repo_dir_for("acme", "widgets", None, 300)
    assert path == f"{fixture_root}/acme/widgets"
    assert is_temp is False


def test_clone_url_token_injection():
    assert gh.clone_url("acme", "widgets", None) == "https://github.com/acme/widgets.git"
    assert gh.clone_url("acme", "widgets", "TKN") == \
        "https://x-access-token:TKN@github.com/acme/widgets.git"
