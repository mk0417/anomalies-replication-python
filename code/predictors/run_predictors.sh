#!/usr/bin/env bash
SECONDS=0
for f in *.py; do python "$f"; done
t=$SECONDS
printf 'Total time taken: %d min %d s\n' "$(( t/60 ))" "$(( t%60 ))"
