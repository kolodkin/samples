#!/usr/bin/env bash
# Download pinned ESM builds of three.js, preact, and htm into web/vendor/.
# Idempotent: skips files that already exist. Required once (needs network).
set -euo pipefail
cd "$(dirname "$0")"

THREE=0.160.0
PREACT=10.19.3
HTM=3.1.1
BASE=https://unpkg.com
DEST=web/vendor

mkdir -p "$DEST/addons/controls" "$DEST/addons/loaders" "$DEST/draco"

fetch() { # url dest
  if [ -f "$2" ]; then echo "have   $2"; return; fi
  echo "fetch  $2"
  curl -fsSL "$1" -o "$2.tmp" && mv "$2.tmp" "$2"
}

fetch "$BASE/three@$THREE/build/three.module.js"                     "$DEST/three.module.js"
fetch "$BASE/three@$THREE/examples/jsm/controls/OrbitControls.js"    "$DEST/addons/controls/OrbitControls.js"
fetch "$BASE/three@$THREE/examples/jsm/loaders/PCDLoader.js"         "$DEST/addons/loaders/PCDLoader.js"
fetch "$BASE/three@$THREE/examples/jsm/loaders/PLYLoader.js"         "$DEST/addons/loaders/PLYLoader.js"
fetch "$BASE/three@$THREE/examples/jsm/loaders/DRACOLoader.js"       "$DEST/addons/loaders/DRACOLoader.js"
fetch "$BASE/three@$THREE/examples/jsm/libs/draco/draco_wasm_wrapper.js" "$DEST/draco/draco_wasm_wrapper.js"
fetch "$BASE/three@$THREE/examples/jsm/libs/draco/draco_decoder.wasm"    "$DEST/draco/draco_decoder.wasm"
fetch "$BASE/three@$THREE/examples/jsm/libs/draco/draco_decoder.js"      "$DEST/draco/draco_decoder.js"
fetch "$BASE/preact@$PREACT/dist/preact.module.js"                   "$DEST/preact.module.js"
fetch "$BASE/preact@$PREACT/hooks/dist/hooks.module.js"              "$DEST/preact-hooks.module.js"
fetch "$BASE/htm@$HTM/dist/htm.module.js"                            "$DEST/htm.module.js"

echo "vendored into $DEST"
