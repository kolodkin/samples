"""End-to-end tests for the archer game (Playwright, Chromium)."""
from playwright.sync_api import expect

# Deterministic, menu-skipping boot used by most tests.
BOOT = "/?autostart=1&seed=42"


def _wait_ready(page):
    page.wait_for_function(
        "() => window.__ARCHER && window.__ARCHER.ready === true", timeout=30000
    )


def test_boot_renders(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["screen"] == "playing"
    # The canvas actually drew something (sky background is excluded).
    assert page.evaluate("() => window.__ARCHER.visiblePixelCount()") > 500


def test_stage_param_and_determinism(server_url, page):
    # Each named stage builds a scene with obstacles; same seed → same layout.
    page.goto(server_url + "/?autostart=1&seed=7&stage=desert")
    _wait_ready(page)
    s1 = page.evaluate("() => window.__ARCHER.state")
    assert s1["stage"] == "desert"
    assert len(s1["obstacles"]) > 5
    assert page.evaluate("() => window.__ARCHER.visiblePixelCount()") > 500

    page.goto(server_url + "/?autostart=1&seed=7&stage=desert")
    _wait_ready(page)
    s2 = page.evaluate("() => window.__ARCHER.state")
    assert s1["obstacles"] == s2["obstacles"]


def test_each_stage_builds(server_url, page):
    for name in ("forest", "desert", "iceberg"):
        page.goto(server_url + f"/?autostart=1&seed=3&stage={name}")
        _wait_ready(page)
        state = page.evaluate("() => window.__ARCHER.state")
        assert state["stage"] == name
        assert len(state["obstacles"]) > 5


def test_bow_draw_charges_and_releases(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower > 0.4", timeout=5000)
    power = page.evaluate("() => window.__ARCHER.state.drawPower")
    assert 0.4 < power <= 1.0
    page.mouse.up()
    assert page.evaluate("() => window.__ARCHER.state.drawPower") == 0


def test_arrow_flies_and_lands(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.fireAt(0, 1, 0)")
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 1
    # Gravity brings it down; ground impact removes it.
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=10000)


def test_mouse_release_fires_arrow(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower > 0.5", timeout=5000)
    page.mouse.up()
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)


def test_arrow_kills_goblin_and_scores(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")  # parked at melee reach
    assert page.evaluate("() => window.__ARCHER.state.enemyCount") == 1
    # Goblin: 40 hp; normal arrow: 34 dmg → two body shots.
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.65, 32)")
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=5000)
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.65, 32)")
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 0", timeout=5000)
    assert page.evaluate("() => window.__ARCHER.state.score") == 100


def test_headshot_double_damage_and_bonus(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    # Head at y=1.3: 34*2=68 >= 40 hp -> one-shot kill, +50 headshot bonus.
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.3, 32)")
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 0", timeout=5000)
    assert page.evaluate("() => window.__ARCHER.state.score") == 150


def test_goblin_advances_and_deals_contact_damage(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 10)")
    z0 = page.evaluate("() => window.__ARCHER.state.enemies[0].z")
    page.wait_for_timeout(1500)
    z1 = page.evaluate("() => window.__ARCHER.state.enemies[0].z")
    assert z1 > z0 + 3  # closing in on the player at z=34
    page.wait_for_function("() => window.__ARCHER.state.hp < 100", timeout=15000)


def test_player_death_shows_game_over(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(5)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)


def _nearest_obstacle_gap(state):
    """Min distance from the first enemy to any obstacle edge."""
    e = state["enemies"][0]
    return min(
        ((o["x"] - e["x"]) ** 2 + (o["z"] - e["z"]) ** 2) ** 0.5 - o["radius"]
        for o in state["obstacles"]
    )


def test_skeleton_takes_cover_behind_obstacle(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    # Spawn already inside firing range so it immediately seeks cover.
    page.evaluate("() => window.__ARCHER.spawnEnemy('skeleton', 0, 12)")
    # It must actually find a cover obstacle (deterministic: seed 42 layout).
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies[0].hasCover", timeout=10000
    )
    page.wait_for_timeout(3000)  # let it walk to its chosen cover
    state = page.evaluate("() => window.__ARCHER.state")
    # Hugging its obstacle (cover point is edge+0.7; a peek adds ~edge+0.5
    # sideways, worst case ~2 m from the edge).
    assert _nearest_obstacle_gap(state) < 2.0


def test_skeleton_shoots_the_player(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.spawnEnemy('skeleton', 0, 12)")
    # Peek/shoot cycle is ~3.4 s; several volleys land within 25 s.
    page.wait_for_function("() => window.__ARCHER.state.hp < 100", timeout=25000)
