import subprocess

from github_exposure_scanner import git_history as gh

from gitutil import make_repo

AWS_KEY = "AKIAIOSFODNN7EXAMPLE"


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


def test_head_blob_shas_reflects_current_tree(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "add two", "date": "2020-01-01",
         "files": {"keep.txt": "keep\n", "gone.txt": "gone\n"}},
        {"message": "remove one", "date": "2020-01-02",
         "files": {"gone.txt": None}},
    ])
    head = gh.head_blob_shas(repo)
    # keep.txt's blob is at HEAD; gone.txt's is not.
    keep_sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD:keep.txt"],
                              check=True, capture_output=True, text=True).stdout.strip()
    assert keep_sha in head
    assert len(head) == 1


def test_iter_blob_history_attributes_introducing_commit(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "add secret", "date": "2020-01-01",
         "files": {"config.py": "KEY = 'v1'\n"}},
        {"message": "unrelated", "date": "2020-06-01",
         "files": {"README.md": "docs\n"}},
        {"message": "change secret", "date": "2021-01-01",
         "files": {"config.py": "KEY = 'v2'\n"}},
    ])
    intros = gh.iter_blob_history(repo)
    v1_sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD~2:config.py"],
                            check=True, capture_output=True, text=True).stdout.strip()
    v2_sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD:config.py"],
                            check=True, capture_output=True, text=True).stdout.strip()
    assert intros[v1_sha].date == "2020-01-01"
    assert intros[v1_sha].path == "config.py"
    assert intros[v1_sha].author == "Test Dev"
    assert intros[v2_sha].date == "2021-01-01"


def test_scan_history_finds_deleted_secret_redacted(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "leak key", "date": "2020-01-01",
         "files": {"src/config.py": f"AWS = '{AWS_KEY}'\n"}},
        {"message": "scrub key", "date": "2020-02-01",
         "files": {"src/config.py": "AWS = ''\n"}},
    ])
    cols = gh.scan_history(repo, "acme", "widgets", stars=1200,
                           max_file_kb=512, detected_at="2026-07-20")
    assert "AWS Key" in cols["secret_type"]
    idx = cols["secret_type"].index("AWS Key")
    assert cols["still_present_at_head"][idx] == 0        # the leaking blob was scrubbed
    assert cols["first_seen"][idx] == "2020-01-01"
    assert cols["commit_author"][idx] == "Test Dev"
    assert cols["path"][idx] == "src/config.py"
    assert AWS_KEY not in "".join(cols["masked_value"])   # still redacted
    assert f"/blob/{cols['commit_sha'][idx]}/src/config.py#L1" in cols["permalink"][idx]


def test_scan_history_skips_binary_blob(tmp_path):
    repo = str(tmp_path / "r")
    make_repo(repo, [
        {"message": "binary", "date": "2020-01-01",
         "files": {"data.bin": "AKIAIOSFODNN7EXAMPLE\x00binary\n"}},
    ])
    cols = gh.scan_history(repo, "acme", "widgets", 1, 512, "2026-07-20")
    assert cols["secret_type"] == []   # NUL byte → skipped before regex
