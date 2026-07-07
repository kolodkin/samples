"""End-to-end tests for the archer game (Playwright, Chromium)."""
import re

from playwright.sync_api import expect

# Deterministic, menu-skipping boot with wave spawning disabled — combat
# tests spawn their own enemies on an empty battlefield.
BOOT = "/?autostart=1&seed=42&waves=0"


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
    # Goblin: 40 hp; normal arrow: 34 dmg → two body shots. Aim below the
    # body center: shooting down from the perch, a y=0.65 aim line grazes
    # the head sphere at exactly the hit threshold and can score a flaky 2×.
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.5, 32)")
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=5000)
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.5, 32)")
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


def test_exploding_arrow_splashes_the_group(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    for x in (-1.2, 0, 1.2):
        page.evaluate(f"() => window.__ARCHER.spawnEnemy('goblin', {x}, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.65, 32, 'exploding')")
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=5000)
    # Direct target dies; every survivor was splashed (hp below the 40 max).
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["enemyCount"] < 3
    assert all(e["hp"] < 40 for e in state["enemies"])


def test_freezing_arrow_stops_attacks_then_thaws(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 0, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.25, 32, 'freezing')")
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies[0] && window.__ARCHER.state.enemies[0].frozen",
        timeout=5000,
    )
    hp0 = page.evaluate("() => window.__ARCHER.state.hp")
    page.wait_for_timeout(1500)  # frozen: no attacks land
    assert page.evaluate("() => window.__ARCHER.state.hp") == hp0
    page.wait_for_function(
        "() => !window.__ARCHER.state.enemies[0].frozen", timeout=5000
    )  # thaws after freezeTime


def test_burning_arrow_ticks_and_spreads(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 0, 32)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 1.8, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.25, 32, 'burning')")
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies[0] && window.__ARCHER.state.enemies[0].burning",
        timeout=5000,
    )
    hp0 = page.evaluate("() => window.__ARCHER.state.enemies[0].hp")
    page.wait_for_timeout(2000)
    hp1 = page.evaluate("() => window.__ARCHER.state.enemies[0].hp")
    assert hp1 < hp0  # damage over time with no further arrows
    # Fire spreads to the adjacent ogre (within spreadRadius).
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies.length > 1 && window.__ARCHER.state.enemies[1].burning",
        timeout=5000,
    )


def test_special_ammo_is_consumed_and_gated(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.keyboard.press("Digit3")
    assert page.evaluate("() => window.__ARCHER.state.selected") == "freezing"
    # No ammo: a full-draw release fizzles.
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower >= 1", timeout=5000)
    page.mouse.up()
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 0
    # With ammo: fires and decrements.
    page.evaluate("() => window.__ARCHER.giveAmmo('freezing', 2)")
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.drawPower >= 1", timeout=5000)
    page.mouse.up()
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)
    assert page.evaluate("() => window.__ARCHER.state.ammo.freezing") == 1


def test_wave_one_spawns_forest_mix(server_url, page):
    page.goto(server_url + "/?autostart=1&seed=42")  # waves ON
    _wait_ready(page)
    # Forest wave 1 = 4 goblins, staggered by spawnInterval.
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 4", timeout=15000)
    assert page.evaluate("() => window.__ARCHER.state.wave") == 1
    types = page.evaluate("() => window.__ARCHER.state.enemies.map(e => e.type)")
    assert types == ["goblin"] * 4


def test_skip_to_wave(server_url, page):
    page.goto(server_url + "/?autostart=1&seed=42")
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.skipToWave(3)")
    # Forest wave 3 = 5 goblins + 2 skeletons.
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 7", timeout=15000)
    assert page.evaluate("() => window.__ARCHER.state.wave") == 3


def test_drops_spawn_and_are_shot_to_collect(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.setDropChance(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.evaluate("() => window.__ARCHER.killAll()")
    page.wait_for_function("() => window.__ARCHER.state.pickupCount === 1", timeout=2000)
    pickup = page.evaluate("() => window.__ARCHER.state.pickups[0]")
    ammo0 = page.evaluate("(t) => window.__ARCHER.state.ammo[t]", pickup["type"])
    page.evaluate(
        "(p) => window.__ARCHER.fireAt(p.x, p.y, p.z)",
        {"x": pickup["x"], "y": pickup["y"], "z": pickup["z"]},
    )
    page.wait_for_function("() => window.__ARCHER.state.pickupCount === 0", timeout=5000)
    ammo1 = page.evaluate("(t) => window.__ARCHER.state.ammo[t]", pickup["type"])
    assert 3 <= ammo1 - ammo0 <= 5


def test_stage_clear_advances_to_desert(server_url, page):
    page.goto(server_url + "/?autostart=1&seed=42")
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setDropChance(0)")
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.skipToWave(5)")
    # Kill every wave-5 enemy as it spawns until the wave is done.
    page.wait_for_function(
        """() => {
          window.__ARCHER.killAll();
          return window.__ARCHER.state.screen === 'stageClear';
        }""",
        timeout=30000,
    )
    page.evaluate("() => window.__ARCHER.nextStage()")
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["stage"] == "desert"
    assert state["screen"] == "playing"
    assert state["hp"] == 100  # HP refills between stages


def test_retry_restores_stage_start_inventory(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    # Ammo gained mid-stage is lost on retry (snapshot from stage start = 0).
    page.evaluate("() => window.__ARCHER.giveAmmo('burning', 7)")
    page.evaluate("() => window.__ARCHER.setPlayerHp(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)
    page.evaluate("() => window.__ARCHER.retryStage()")
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["screen"] == "playing"
    assert state["hp"] == 100
    assert state["ammo"]["burning"] == 0
    assert state["enemyCount"] == 0  # battlefield cleared


def test_multikill_combo_bonus(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.setDropChance(0)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', -1, 32)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 1, 32)")
    page.evaluate("() => window.__ARCHER.killAll()")  # same-frame kills chain a combo
    # 100 + (100 + 25 combo bonus on the second kill)
    page.wait_for_function("() => window.__ARCHER.state.score === 225", timeout=5000)


def test_best_score_persists_across_reloads(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.3, 32)")  # headshot: 150 points
    page.wait_for_function("() => window.__ARCHER.state.score === 150", timeout=5000)
    page.evaluate("() => window.__ARCHER.setPlayerHp(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)
    page.goto(server_url + BOOT)  # fresh page, same origin -> same localStorage
    _wait_ready(page)
    assert page.evaluate("() => window.__ARCHER.state.best.score") >= 150


def test_title_screen_and_start_button(server_url, page):
    page.goto(server_url + "/?seed=1&waves=0")  # no autostart: land on the title
    _wait_ready(page)
    expect(page.get_by_test_id("title-screen")).to_be_visible()
    page.get_by_test_id("start-btn").click()
    page.wait_for_function("() => window.__ARCHER.state.screen === 'playing'", timeout=5000)
    expect(page.get_by_test_id("hud")).to_be_visible()


def test_hud_reflects_score_ammo_and_selection(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.3, 32)")  # headshot kill: 150
    page.wait_for_function("() => window.__ARCHER.state.score === 150", timeout=5000)
    expect(page.get_by_test_id("score")).to_have_text("150")
    page.evaluate("() => window.__ARCHER.giveAmmo('freezing', 4)")
    expect(page.get_by_test_id("ammo-freezing")).to_have_text("4")
    page.keyboard.press("Digit3")
    expect(page.get_by_test_id("slot-freezing")).to_have_class(re.compile(r"\bactive\b"))
    expect(page.get_by_test_id("wave")).to_contain_text("forest")


def test_game_over_screen_retry_button(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)
    expect(page.get_by_test_id("gameover-screen")).to_be_visible()
    page.get_by_test_id("retry-btn").click()
    page.wait_for_function("() => window.__ARCHER.state.screen === 'playing'", timeout=5000)
    assert page.evaluate("() => window.__ARCHER.state.hp") == 100
