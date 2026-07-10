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


def _drop_and_shoot_pickup(page):
    """Kill an inert goblin, shoot the pickup it drops, return the pickup."""
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32, true)")
    page.evaluate("() => window.__ARCHER.killAll()")
    page.wait_for_function("() => window.__ARCHER.state.pickupCount === 1", timeout=2000)
    pickup = page.evaluate("() => window.__ARCHER.state.pickups[0]")
    page.evaluate("(p) => window.__ARCHER.fireAt(p.x, p.y, p.z)", pickup)
    page.wait_for_function("() => window.__ARCHER.state.pickupCount === 0", timeout=5000)
    return pickup


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


def test_perch_visible_underfoot(server_url, page):
    # The platform under the archer must read as a platform: the pixel at the
    # player's feet (the perch top cap) differs from the open terrain nearby.
    for name in ("forest", "desert", "iceberg"):
        page.goto(server_url + f"/?autostart=1&seed=3&stage={name}&waves=0")
        _wait_ready(page)
        perch = page.evaluate("() => window.__ARCHER.pixelAt(0.5, 0.97)")
        ground = page.evaluate("() => window.__ARCHER.pixelAt(0.55, 0.65)")
        diff = sum(abs(a - b) for a, b in zip(perch, ground))
        assert diff > 24, f"{name}: perch top blends into terrain ({perch} vs {ground})"


def test_power_buttons_adjust_and_clamp(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    assert page.evaluate("() => window.__ARCHER.state.power") == 0.6
    page.get_by_test_id("power-up").dispatch_event("pointerdown")
    assert page.evaluate("() => window.__ARCHER.state.power") == 0.7
    expect(page.get_by_test_id("power-value")).to_have_text("70%")
    for _ in range(5):  # clamps at max
        page.get_by_test_id("power-up").dispatch_event("pointerdown")
    assert page.evaluate("() => window.__ARCHER.state.power") == 1.0
    for _ in range(12):  # clamps at min
        page.get_by_test_id("power-down").dispatch_event("pointerdown")
    assert page.evaluate("() => window.__ARCHER.state.power") == 0.2


def test_power_keys_adjust_power(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.keyboard.press("Equal")
    assert page.evaluate("() => window.__ARCHER.state.power") == 0.7
    page.keyboard.press("Minus")
    page.keyboard.press("Minus")
    assert page.evaluate("() => window.__ARCHER.state.power") == 0.5


def test_arrow_flies_and_lands(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.fireAt(0, 1, 0)")
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 1
    # Gravity brings it down; ground impact removes it.
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=10000)


def test_click_fires_arrow(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.mouse.move(640, 360)
    page.mouse.down()
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)
    page.mouse.up()
    # Power is a persistent setting; firing does not reset it.
    assert page.evaluate("() => window.__ARCHER.state.power") == 0.6


def test_shot_arrow_launches_from_bow_with_trail(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    # Fire via the input path and read state in the same synchronous task —
    # no frame has run yet, so the arrow still sits at its visual spawn.
    offset = page.evaluate(
        """() => {
          const canvas = document.getElementById('game');
          canvas.dispatchEvent(new MouseEvent('mousedown', { button: 0, bubbles: true }));
          const a = window.__ARCHER.state.arrows[0];
          return Math.hypot(a.visX - a.x, a.visY - a.y, a.visZ - a.z);
        }"""
    )
    # The projectile appears at the bow, not centered on the aim line, so
    # the shot reads as an arrow leaving the bow instead of a dot.
    assert offset > 0.2
    # It converges onto the true flight line while a tracer trail builds up.
    page.wait_for_function(
        """() => {
          const a = window.__ARCHER.state.arrows[0];
          return !a || (a.trailPoints >= 4
            && Math.hypot(a.visX - a.x, a.visY - a.y, a.visZ - a.z) < 0.05);
        }""",
        timeout=5000,
    )
    # Physics is untouched: the arrow still lands and expires.
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=10000)


def test_shot_releases_arrow_then_renocks(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    # At rest an arrow sits nocked on the bow, ready to shoot.
    assert page.evaluate("() => window.__ARCHER.state.nocked") is True
    assert page.evaluate("() => window.__ARCHER.state.canShoot") is True
    page.mouse.move(640, 360)
    page.mouse.down()
    # The nocked arrow leaves the bow the moment the shot fires — the bow
    # is briefly empty while the projectile flies.
    page.wait_for_function(
        "() => window.__ARCHER.state.arrowCount === 1"
        " && window.__ARCHER.state.nocked === false",
        timeout=2000,
    )
    page.mouse.up()
    # A fresh arrow appears and is drawn back, re-arming the shot.
    page.wait_for_function("() => window.__ARCHER.state.nocked === true", timeout=2000)
    page.wait_for_function("() => window.__ARCHER.state.canShoot === true", timeout=2000)


def test_rapid_clicks_gated_by_renock(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.mouse.move(640, 360)
    # A synchronous burst of clicks (no frames in between): only the first
    # fires; the rest land mid-reload and are swallowed.
    shots = page.evaluate(
        """() => {
          const canvas = document.getElementById('game');
          const n0 = window.__ARCHER.state.arrowCount;
          for (let i = 0; i < 5; i++) {
            canvas.dispatchEvent(new MouseEvent('mousedown', { button: 0, bubbles: true }));
          }
          return window.__ARCHER.state.arrowCount - n0;
        }"""
    )
    assert shots == 1
    # Once the new arrow is nocked (and the first shot has landed), the
    # bow shoots again.
    page.wait_for_function(
        "() => window.__ARCHER.state.canShoot === true"
        " && window.__ARCHER.state.arrowCount === 0",
        timeout=10000,
    )
    page.mouse.down()
    page.mouse.up()
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)


def test_arrow_kills_goblin_and_scores(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    # Inert target dummy: real goblins strike once at melee reach and vanish.
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32, true)")
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
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32, true)")
    # Head at y=1.3: 34*2=68 >= 40 hp -> one-shot kill, +50 headshot bonus.
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.3, 32)")
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 0", timeout=5000)
    assert page.evaluate("() => window.__ARCHER.state.score") == 150


def test_goblin_advances_hits_once_and_despawns(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 10)")
    z0 = page.evaluate("() => window.__ARCHER.state.enemies[0].z")
    page.wait_for_timeout(1500)
    z1 = page.evaluate("() => window.__ARCHER.state.enemies[0].z")
    assert z1 > z0 + 3  # closing in on the player at z=34
    page.wait_for_function("() => window.__ARCHER.state.hp < 100", timeout=15000)
    # One strike and the monster is spent: it disappears after landing its hit.
    page.wait_for_function("() => window.__ARCHER.state.enemyCount === 0", timeout=2000)
    assert page.evaluate("() => window.__ARCHER.state.hp") == 90  # exactly one hit


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
        page.evaluate(f"() => window.__ARCHER.spawnEnemy('goblin', {x}, 32, true)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 0.65, 32, 'exploding')")
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 0", timeout=5000)
    # Direct target dies; every survivor was splashed (hp below the 40 max).
    state = page.evaluate("() => window.__ARCHER.state")
    assert state["enemyCount"] < 3
    assert all(e["hp"] < 40 for e in state["enemies"])


def test_freezing_arrow_halts_advance_then_thaws(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    # Spawn out of melee reach and freeze it mid-advance (the wide body
    # sphere absorbs the ~0.3 m it walks during the arrow's flight).
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 0, 26)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.25, 26, 'freezing')")
    page.wait_for_function(
        "() => window.__ARCHER.state.enemies[0] && window.__ARCHER.state.enemies[0].frozen",
        timeout=5000,
    )
    z0 = page.evaluate("() => window.__ARCHER.state.enemies[0].z")
    page.wait_for_timeout(1500)  # frozen solid: no advance, no attacks
    assert page.evaluate("() => window.__ARCHER.state.enemies[0].z") == z0
    assert page.evaluate("() => window.__ARCHER.state.hp") == 100
    page.wait_for_function(
        "() => !window.__ARCHER.state.enemies[0].frozen", timeout=5000
    )  # thaws after freezeTime


def test_burning_arrow_ticks_and_spreads(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 0, 32, true)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('ogre', 1.8, 32, true)")
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
    # No ammo: the click fizzles.
    page.mouse.move(640, 360)
    page.mouse.click(640, 360)
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 0
    # With ammo: fires and decrements.
    page.evaluate("() => window.__ARCHER.giveAmmo('freezing', 2)")
    page.mouse.click(640, 360)
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)
    assert page.evaluate("() => window.__ARCHER.state.ammo.freezing") == 1


def test_spending_last_special_arrow_falls_back_to_normal(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.giveAmmo('freezing', 1)")
    page.keyboard.press("Digit3")
    assert page.evaluate("() => window.__ARCHER.state.selected") == "freezing"
    # Firing the last special arrow auto-selects the basic (normal) arrow.
    page.mouse.move(640, 360)
    page.mouse.click(640, 360)
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)
    assert page.evaluate("() => window.__ARCHER.state.ammo.freezing") == 0
    assert page.evaluate("() => window.__ARCHER.state.selected") == "normal"


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
    page.evaluate("() => window.__ARCHER.setHealChance(0)")  # force an ammo drop
    ammo0 = page.evaluate("() => window.__ARCHER.state.ammo")
    pickup = _drop_and_shoot_pickup(page)
    ammo1 = page.evaluate("() => window.__ARCHER.state.ammo")
    assert 3 <= ammo1[pickup["type"]] - ammo0[pickup["type"]] <= 5


def test_heal_potion_drop_restores_hp_capped_at_max(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setDropChance(1)")
    page.evaluate("() => window.__ARCHER.setHealChance(1)")  # force a potion drop
    # Wounded player: the potion restores CONFIG.drops.heal.amount HP.
    page.evaluate("() => window.__ARCHER.setPlayerHp(50)")
    assert _drop_and_shoot_pickup(page)["type"] == "heal"
    assert page.evaluate("() => window.__ARCHER.state.hp") == 75
    # Near full: healing clamps at max HP instead of overhealing.
    page.evaluate("() => window.__ARCHER.setPlayerHp(90)")
    _drop_and_shoot_pickup(page)
    assert page.evaluate("() => window.__ARCHER.state.hp") == 100


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
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', -1, 32, true)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 1, 32, true)")
    page.evaluate("() => window.__ARCHER.killAll()")  # same-frame kills chain a combo
    # 100 + (100 + 25 combo bonus on the second kill)
    page.wait_for_function("() => window.__ARCHER.state.score === 225", timeout=5000)


def test_best_score_persists_across_reloads(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.evaluate("() => window.__ARCHER.setPlayerHp(10000)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32, true)")
    page.evaluate("() => window.__ARCHER.fireAt(0, 1.3, 32)")  # headshot: 150 points
    page.wait_for_function("() => window.__ARCHER.state.score === 150", timeout=5000)
    page.evaluate("() => window.__ARCHER.setPlayerHp(1)")
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32)")
    page.wait_for_function("() => window.__ARCHER.state.screen === 'gameOver'", timeout=10000)
    page.goto(server_url + BOOT)  # fresh page, same origin -> same localStorage
    _wait_ready(page)
    assert page.evaluate("() => window.__ARCHER.state.best.score") >= 150


def _touch(page, type_, x, y, identifier=0):
    """Dispatch a synthetic single-finger TouchEvent on the game canvas."""
    page.evaluate(
        """(a) => {
          const canvas = document.getElementById('game');
          const touch = new Touch({
            identifier: a.id, target: canvas, clientX: a.x, clientY: a.y,
          });
          const live = a.type === 'touchend' || a.type === 'touchcancel' ? [] : [touch];
          canvas.dispatchEvent(new TouchEvent(a.type, {
            bubbles: true, cancelable: true,
            touches: live, targetTouches: live, changedTouches: [touch],
          }));
        }""",
        {"type": type_, "x": x, "y": y, "id": identifier},
    )


def test_touch_drag_aims_without_firing(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    yaw0 = page.evaluate("() => window.__ARCHER.state.yaw")
    _touch(page, "touchstart", 400, 300)
    _touch(page, "touchmove", 250, 340)
    _touch(page, "touchend", 250, 340)
    assert page.evaluate("() => window.__ARCHER.state.yaw") != yaw0
    # Looking around never looses an arrow: the drag blows the tap budget.
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 0


def test_touch_tap_on_canvas_shoots(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    _touch(page, "touchstart", 400, 300)
    _touch(page, "touchend", 400, 300)
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)


def test_touch_long_press_does_not_shoot(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    _touch(page, "touchstart", 400, 300)
    page.wait_for_timeout(400)  # past CONFIG.touch.tapMaxMs
    _touch(page, "touchend", 400, 300)
    assert page.evaluate("() => window.__ARCHER.state.arrowCount") == 0


def test_touch_enables_touch_hud(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    expect(page.get_by_test_id("fire-btn")).not_to_be_attached()
    _touch(page, "touchstart", 400, 300)
    _touch(page, "touchend", 400, 300)
    expect(page.get_by_test_id("fire-btn")).to_be_visible()
    expect(page.get_by_test_id("pause-btn")).to_be_visible()


def test_fire_button_shoots_on_tap(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    # Enter touch mode with a drag (a tap would itself fire and mask the
    # button's shot behind the reload gate).
    _touch(page, "touchstart", 400, 300)
    _touch(page, "touchmove", 300, 300)
    _touch(page, "touchend", 300, 300)
    page.get_by_test_id("fire-btn").dispatch_event("pointerdown")
    page.wait_for_function("() => window.__ARCHER.state.arrowCount === 1", timeout=2000)


def test_quiver_tap_switches_arrow(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    page.get_by_test_id("slot-exploding").click()
    assert page.evaluate("() => window.__ARCHER.state.selected") == "exploding"
    expect(page.get_by_test_id("slot-exploding")).to_have_class(re.compile(r"\bactive\b"))


def test_touch_pause_and_resume(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    _touch(page, "touchstart", 400, 300)
    _touch(page, "touchend", 400, 300)
    page.get_by_test_id("pause-btn").click()
    assert page.evaluate("() => window.__ARCHER.state.screen") == "paused"
    page.get_by_test_id("resume-btn").click()
    page.wait_for_function("() => window.__ARCHER.state.screen === 'playing'", timeout=5000)


def test_bow_viewmodel_stays_on_screen_in_portrait(server_url, page):
    page.goto(server_url + BOOT)
    _wait_ready(page)
    # Wide viewport: the bow rests at its designed off-center offset.
    page.wait_for_function("() => window.__ARCHER.state.bowX === 0.26", timeout=5000)
    page.set_viewport_size({"width": 450, "height": 900})
    # The 70° FOV is vertical, so at the bow's 0.6 m depth the visible
    # half-width is tan(35°)·0.6·aspect ≈ 0.21 — the bow (≈0.12 half-extent)
    # must slide inward to stay fully in frame.
    page.wait_for_function(
        """() => {
          const halfW = Math.tan(35 * Math.PI / 180) * 0.6 * (450 / 900);
          return window.__ARCHER.state.bowX + 0.12 <= halfW;
        }""",
        timeout=5000,
    )


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
    page.evaluate("() => window.__ARCHER.spawnEnemy('goblin', 0, 32, true)")
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
