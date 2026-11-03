#!/usr/bin/env bash
# Download the SIFT-1M benchmark (Jegou, Douze and Schmid, 2011) into data/sift/.
# 1,000,000 base vectors, 10,000 queries and their exact 100 nearest neighbours, 128 dimensions.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data
if [ ! -f data/sift/sift_base.fvecs ]; then
  curl -fL --retry 3 -o data/sift.tar.gz ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
  tar -xzf data/sift.tar.gz -C data
  rm data/sift.tar.gz
fi
ls -l data/sift
