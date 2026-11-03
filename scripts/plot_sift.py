"""Plot recall@10 against queries per second from a saved `./gradlew sift` run.

    python scripts/plot_sift.py results/sift-1m.txt docs/sift-1m-recall-qps.png   (needs matplotlib)
"""

import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

source, out = sys.argv[1], sys.argv[2]
rows = re.findall(r"^\| (HNSW|Exact \(FlatIndex\)) \| (\S+) \| ([\d.]+) \| ([\d,]+) \|", open(source).read(), re.M)
hnsw = [(float(r), float(q.replace(",", "")), ef) for name, ef, r, q in rows if name == "HNSW"]
exact = [(float(r), float(q.replace(",", ""))) for name, _, r, q in rows if name != "HNSW"]

fig, ax = plt.subplots(figsize=(7, 4), dpi=130)
ax.plot([h[0] for h in hnsw], [h[1] for h in hnsw], marker="o", color="#2563eb", label="HNSW (M = 16)")
for recall, qps, ef in hnsw:
    ax.annotate(f"ef={ef}", (recall, qps), textcoords="offset points", xytext=(4, 4), fontsize=7, color="#334155")
ax.scatter([e[0] for e in exact], [e[1] for e in exact], color="#0f172a", marker="s", label="exact search", zorder=3)
ax.set_yscale("log")
ax.set_xlabel("recall@10")
ax.set_ylabel("queries per second (log scale, single thread)")
ax.set_title("SIFT-1M: recall against speed")
ax.legend(frameon=False, loc="lower left")
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
fig.tight_layout()
fig.savefig(out)
print(f"wrote {out}")
