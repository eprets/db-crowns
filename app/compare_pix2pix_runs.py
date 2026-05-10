# app/compare_pix2pix_runs.py

from pathlib import Path


def _read_metrics(metrics_path: Path):
    if not metrics_path.exists():
        return None

    result = {}

    text = metrics_path.read_text(encoding="utf-8")

    for line in text.splitlines():
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        result[key.strip()] = value.strip()

    return result


def compare_pix2pix_runs(runs_root: Path) -> None:
    runs = sorted([p for p in runs_root.iterdir() if p.is_dir()])

    print("\n=== PIX2PIX RUNS COMPARISON ===\n")
    print(f"{'run':<15} | {'n_test':<8} | {'MAE':<12} | {'PSNR':<12}")
    print("-" * 58)

    for run_dir in runs:
        metrics_path = run_dir / "eval_test" / "metrics.txt"
        metrics = _read_metrics(metrics_path)

        if metrics is None:
            print(f"{run_dir.name:<15} | {'NO EVAL':<8} | {'-':<12} | {'-':<12}")
            continue

        n_test = metrics.get("n_test", "-")
        mae = metrics.get("MAE_mean", "-")
        psnr = metrics.get("PSNR_mean", "-")

        print(f"{run_dir.name:<15} | {n_test:<8} | {mae:<12} | {psnr:<12}")