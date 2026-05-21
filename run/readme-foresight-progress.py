from pathlib import Path
import os
import shutil

import pandas as pd

RUN_DIR = Path(__file__).resolve().parent
REPO_DIR = RUN_DIR.parent
os.environ["PSIM_BASE_DIR"] = str(REPO_DIR)

from utils.exp_runner import do_experiment
from utils.util import get_incremented_number


def copy_internal_plots(results_dir, figures_dir):
    figures_dir.mkdir(parents=True, exist_ok=True)

    selected_plots = {
        "base-runtime-link-load.png": lambda path: (
            path.name == "demand_runtime_10_all.png" and "c-base" in str(path)
        ),
        "foresight-runtime-link-load.png": lambda path: (
            path.name == "demand_runtime_10_all.png" and "c-coloring-v7" in str(path)
        ),
        "foresight-final-demand.png": lambda path: (
            path.name == "demand_final.png" and "c-coloring-v7" in str(path)
        ),
        "foresight-routing-merged-ranges.png": lambda path: (
            path.name == "merged_ranges_0.png" and "graph-coloring-v7" in str(path)
        ),
        "foresight-routing-rack-dependency.png": lambda path: (
            path.name == "rack_dependency_0.png" and "graph-coloring-v7" in str(path)
        ),
    }

    all_pngs = sorted(results_dir.glob("**/*.png"))
    copied = []
    for destination_name, matcher in selected_plots.items():
        source = next((path for path in all_pngs if matcher(path)), None)
        if source is None:
            continue

        destination = figures_dir / destination_name
        shutil.copy2(source, destination)
        copied.append(destination)

    manifest_path = figures_dir / "MANIFEST.txt"
    with manifest_path.open("w") as manifest:
        for path in copied:
            manifest.write(f"{path.name}\n")

    return copied


if __name__ == "__main__":
    figures_dir = REPO_DIR / "docs" / "figures" / "foresight-progress"
    os.chdir(RUN_DIR)

    seed_range = 1
    scale = 100
    exp_number = get_incremented_number("readme-number.txt")

    summary, results_dir = do_experiment(
        seed_range=seed_range,
        added_comparisons=["coloring-v7"],
        experiment_seed=777,
        worker_thread_count=1,
        plot_stuff=True,
        sim_length=60 * scale,
        machine_count=12,
        rack_size=4,
        job_sizes=(4, 4),
        placement_mode="entropy",
        ring_mode="letitbe",
        desired_entropy=0.5,
        oversub=2,
        cmmcmp_range=(1.5, 2),
        fallback_threshold=0.5,
        comm_size=(120 * scale, 360 * scale, 60 * scale),
        comp_size=(2 * scale, 10 * scale, 1 * scale),
        layer_count=(1, 2, 1),
        punish_oversubscribed_min=1,
        min_rate=100,
        inflate=1,
    )

    results_dir = Path(results_dir)
    copied = copy_internal_plots(results_dir, figures_dir)

    summary_path = figures_dir / f"readme-summary-{exp_number}.csv"
    pd.DataFrame(summary).to_csv(summary_path, index=False)

    print(f"Results directory: {results_dir}")
    print(f"Copied {len(copied)} internal plots to {figures_dir}")
    for path in copied:
        print(f" - {path}")
    print(f"Summary: {summary_path}")
