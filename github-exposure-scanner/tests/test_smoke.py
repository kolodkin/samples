import github_exposure_scanner


def test_package_imports():
    assert hasattr(github_exposure_scanner, "main")
