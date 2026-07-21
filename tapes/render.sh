#!/bin/bash
# Render the docs terminal GIFs with VHS inside Docker (no host deps beyond Docker).
# Usage: tapes/render.sh [quickstart checkpoint-resume backfill]
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=dlt-ops-vhs

docker build -f tapes/Dockerfile -t "$IMG" .
mkdir -p docs/assets/terminal

tapes=("$@")
[ ${#tapes[@]} -eq 0 ] && tapes=(quickstart checkpoint-resume backfill)
for name in "${tapes[@]}"; do
    # checkpoint-resume records from the checkout root (it copies
    # examples/basic_project out); the other tapes start in /tmp.
    workdir=/tmp
    [ "$name" = "checkpoint-resume" ] && workdir=/repo
    docker run --rm \
        -v "$PWD":/repo:ro \
        -v "$PWD/docs/assets/terminal":/out \
        -w "$workdir" \
        "$IMG" "/repo/tapes/$name.tape"
    # Recompress: diff-based 64-color palette, then lossy LZW optimization.
    docker run --rm \
        -v "$PWD/docs/assets/terminal":/out \
        --entrypoint /bin/bash \
        "$IMG" -c "
            set -e
            ffmpeg -hide_banner -loglevel error -i /out/$name.gif \
                -vf 'split[a][b];[a]palettegen=max_colors=64:stats_mode=diff[p];[b][p]paletteuse=dither=none:diff_mode=rectangle' \
                -y /tmp/$name.gif
            gifsicle -O3 --lossy=70 /tmp/$name.gif -o /out/$name.gif
        "
done
ls -la docs/assets/terminal
