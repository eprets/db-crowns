import logging
import sys
from pathlib import Path

from app.config import load_config
from app.logging_setup import setup_logging
from app.db.init_db import init_db
from app.import_images import import_images
from app.db.queries import list_images, count_images
from app.annotator_ellipse import EllipseAnnotator
from app.db.queries import list_annotations, count_annotations
from app.build_observations import build_observations
from app.db.maintenance import deduplicate_annotations_keep_latest
from app.db.maintenance_obs import cleanup_orphan_observations
from app.db.queries import list_observations, count_observations
from app.show_observation import show_observation
#from app.backfill_obs_height import backfill_obs_height_from_images
from app.check_heights import print_heights_summary
from app.fill_flight_altitude import fill_flight_altitude_from_filename
from app.build_levels import build_levels, show_levels
from app.normalize_scale import normalize_scale
from app.synthesize_missing import synthesize_missing_levels
from app.export_dataset_pairs import export_pix2pix_pairs
from app.backfill_obs_height import backfill_obs_height
from app.export_pix2pix_pairs import export_pix2pix_pairs
from app.split_pix2pix_dataset import split_pix2pix_dataset
from app.gan.train_pix2pix import train_pix2pix
from app.gan.eval_pix2pix import eval_pix2pix
from app.gan.apply_pix2pix import apply_pix2pix_one
from app.db.migrate_crown_levels_pix2pix import migrate_crown_levels_for_pix2pix

def main():
    # Загружаем конфигурацию
    config = load_config("configs/config.yaml")

    # Настраиваем логирование
    setup_logging(
        level=config["logging"]["level"],
        log_file=Path(config["logging"]["log_file"])
    )

    db_path = Path(config["paths"]["db_path"])
    schema_path = Path("app/db/schema.sql")
    raw_images_dir = Path(config["paths"]["raw_images_dir"])

    logging.info(
        "Starting project: %s v%s",
        config["project"]["name"],
        config["project"]["version"]
    )

    # Инициализируем БД
    init_db(db_path=db_path, schema_path=schema_path)
    logging.info("Database ready: %s", db_path)

    # ===== РЕЖИМ: IMPORT =====
    # python -m app.main import
    if len(sys.argv) >= 2 and sys.argv[1] == "import":
        added = import_images(
            db_path=db_path,
            raw_images_dir=raw_images_dir
        )
        logging.info("Import finished. Added %d images.", added)
        print(f"Imported {added} images.")
        return

    # ===== РЕЖИМ: LIST IMAGES =====
    # python -m app.main list-images
    if len(sys.argv) >= 2 and sys.argv[1] == "list-images":
        total = count_images(db_path)
        rows = list_images(db_path, limit=20)

        print(f"\nTotal images in DB: {total}")
        print("Last images:")
        for r in rows:
            print("-", r["image_id"], "|", r["path"])
        return

    # python -m app.main annotate <tree_id> <tree_type>
    if len(sys.argv) >= 2 and sys.argv[1] == "annotate":
        if len(sys.argv) < 4:
            print("Usage: python -m app.main annotate <tree_id> <tree_type>")
            print("Example: python -m app.main annotate tree_001 pine")
            return

        tree_id = sys.argv[2]
        tree_type = sys.argv[3]

        annotator = EllipseAnnotator(db_path=db_path)
        annotator.run(tree_id=tree_id, tree_type=tree_type)
        return

    # python -m app.main list-annotations
    if len(sys.argv) >= 2 and sys.argv[1] == "list-annotations":
        total = count_annotations(db_path)
        rows = list_annotations(db_path, limit=20)

        print(f"\nTotal annotations in DB: {total}")
        print("Last annotations:")
        for r in rows:
            theta_deg = float(r["theta"]) * 180.0 / 3.1415926535
            print(
                f"- ann_id={r['annotation_id']} | tree_id={r['tree_id']} ({r['tree_type']}) "
                f"| image={r['path']}\n"
                f"  ellipse: x0={r['x0']:.1f} y0={r['y0']:.1f} a={r['a']:.1f} b={r['b']:.1f} theta={theta_deg:.1f}deg"
            )
        return

    # python -m app.main build-observations
    if len(sys.argv) >= 2 and sys.argv[1] == "build-observations":
        roi_raw_dir = Path(config["paths"]["roi_raw_dir"])
        padding_px = int(config["roi"]["padding_px"])

        added = build_observations(
            db_path=db_path,
            roi_raw_dir=roi_raw_dir,
            padding_px=padding_px,
            limit=None
        )
        logging.info("Build observations finished. Added %d observations.", added)
        print(f"Built {added} observations.")
        return

    # python -m app.main dedup-annotations
    if len(sys.argv) >= 2 and sys.argv[1] == "dedup-annotations":
        removed = deduplicate_annotations_keep_latest(db_path)
        logging.info("Dedup annotations done. Removed %d rows.", removed)
        print(f"Dedup done. Removed {removed} duplicate annotations.")
        return

    # python -m app.main cleanup-observations
    if len(sys.argv) >= 2 and sys.argv[1] == "cleanup-observations":
        removed = cleanup_orphan_observations(db_path)
        logging.info("Cleanup observations done. Removed %d orphan rows.", removed)
        print(f"Cleanup done. Removed {removed} orphan observations.")
        return

    # python -m app.main list-observations
    if len(sys.argv) >= 2 and sys.argv[1] == "list-observations":
        total = count_observations(db_path)
        rows = list_observations(db_path, limit=20)

        print(f"\nTotal observations in DB: {total}")
        print("Last observations:")
        for r in rows:
            feats = r.get("features", {})
            print(f"  ellipse_area={feats.get('ellipse_area', 'NA')}  axis_ratio={feats.get('axis_ratio', 'NA')}")
            print(f"- obs_id={r['obs_id']} | tree_id={r['tree_id']}")
            print(f"  roi={r['roi_raw_path']}")
            #print(f"  ellipse_area={feats.get('ellipse_area')}  axis_ratio={feats.get('axis_ratio')}")
        return

    # python -m app.main show-observation <obs_id>
    if len(sys.argv) >= 2 and sys.argv[1] == "show-observation":
        if len(sys.argv) < 3:
            print("Usage: python -m app.main show-observation <obs_id>")
            return
        show_observation(db_path=db_path, obs_id=sys.argv[2])
        return

    # python -m app.main backfill-obs-height
    #if len(sys.argv) >= 2 and sys.argv[1] == "backfill-obs-height":
     #   updated = backfill_obs_height_from_images(db_path)
     #   print(f"Backfill done. Updated {updated} observations.")
     #   return

    # python -m app.main check-heights
    if len(sys.argv) >= 2 and sys.argv[1] == "check-heights":
        print_heights_summary(db_path=db_path, limit=20)
        return

    # python -m app.main fill-flight-altitude-from-filename
    if len(sys.argv) >= 2 and sys.argv[1] == "fill-flight-altitude-from-filename":
        updated = fill_flight_altitude_from_filename(db_path)
        print(f"Updated {updated} images (flight_altitude from filename).")
        return

    # python -m app.main build-levels
    if len(sys.argv) >= 2 and sys.argv[1] == "build-levels":
        levels = [float(x) for x in config["heights_grid"]["levels_m"]]
        added = build_levels(db_path=db_path, levels=levels)
        print(f"Build levels done. Upserted {added} rows.")
        return

    # python -m app.main show-levels <tree_id>
    if len(sys.argv) >= 2 and sys.argv[1] == "show-levels":
        if len(sys.argv) < 3:
            print("Usage: python -m app.main show-levels <tree_id>")
            return
        tree_id = sys.argv[2]
        levels = [float(x) for x in config["heights_grid"]["levels_m"]]
        show_levels(db_path=db_path, tree_id=tree_id, levels=levels)
        return

    # python -m app.main show-levels <tree_id>
    if len(sys.argv) >= 2 and sys.argv[1] == "show-levels":
        if len(sys.argv) < 3:
            print("Usage: python -m app.main show-levels <tree_id>")
            return
        tree_id = sys.argv[2]
        levels = [float(x) for x in config["heights_grid"]["levels_m"]]
        show_levels(db_path=db_path, tree_id=tree_id, levels=levels)
        return

    # python -m app.main normalize-scale
    if len(sys.argv) >= 2 and sys.argv[1] == "normalize-scale":
        roi_norm_dir = Path(config["paths"]["roi_norm_dir"])
        out_size = tuple(config["roi"]["out_size"])  # [256,256] -> (256,256)

        processed = normalize_scale(
            db_path=db_path,
            roi_norm_dir=roi_norm_dir,
            out_size=out_size,
            only_missing=True
        )
        print(f"Normalize scale done. Processed {processed} levels.")
        return

    # python -m app.main synthesize-missing [tree_id] [level]
    # Примеры:
    #   python -m app.main synthesize-missing
    #   python -m app.main synthesize-missing tree_001
    #   python -m app.main synthesize-missing tree_001 20
    if len(sys.argv) >= 2 and sys.argv[1] == "synthesize-missing":
        levels = [float(x) for x in config["heights_grid"]["levels_m"]]
        roi_norm_dir = Path(config["paths"]["roi_norm_dir"])

        only_tree_id = None
        fill_only_levels = None

        if len(sys.argv) >= 3:
            only_tree_id = sys.argv[2]

        if len(sys.argv) >= 4:
            fill_only_levels = [float(sys.argv[3])]

        created = synthesize_missing_levels(
            db_path=db_path,
            levels_grid=levels,
            roi_norm_dir=roi_norm_dir,
            only_tree_id=only_tree_id,
            fill_only_levels=fill_only_levels,
            overwrite_existing_synth=False,
        )
        print(f"Synthesize done. Created/updated {created} synth levels.")
        return

    # python -m app.main export-dataset-pairs [only_tree_id]
    # Примеры:
    #   python -m app.main export-dataset-pairs
    #   python -m app.main export-dataset-pairs tree_001
    if len(sys.argv) >= 2 and sys.argv[1] == "export-dataset-pairs":
        only_tree_id = None
        if len(sys.argv) >= 3:
            only_tree_id = sys.argv[2]

        levels = [float(x) for x in config["heights_grid"]["levels_m"]]

        out_dir = Path("data/datasets/pix2pix_pairs")

        exported = export_pix2pix_pairs(
            db_path=db_path,
            out_dir=out_dir,
            levels_grid=levels,
            pair_mode="neighbors",
            train_ratio=0.8,
            val_ratio=0.1,
            test_ratio=0.1,
            seed=42,
            only_tree_id=only_tree_id,
        )
        print(f"Export dataset pairs done. Exported {exported} pairs to {out_dir}.")
        return

    # python -m app.main backfill-obs-height
    if len(sys.argv) >= 2 and sys.argv[1] == "backfill-obs-height":
        updated = backfill_obs_height(db_path)
        print(f"Backfill done. Updated {updated} observations.")
        return

    # python -m app.main export-pix2pix
    if len(sys.argv) >= 2 and sys.argv[1] == "export-pix2pix":
        out_dir = Path(config["pix2pix"]["out_dir"])
        result = export_pix2pix_pairs(
            db_path=db_path,
            out_dir=out_dir,
            neighbor_max_gap_m=float(config["pix2pix"]["neighbor_max_gap_m"]),
            far_min_gap_m=float(config["pix2pix"]["far_min_gap_m"]),
            far_max_gap_m=float(config["pix2pix"]["far_max_gap_m"]),
            include_reverse=bool(config["pix2pix"]["include_reverse"]),
            max_pairs_per_tree=int(config["pix2pix"]["max_pairs_per_tree"]),
            include_synth=bool(config["pix2pix"]["include_synth"]),
            image_ext=str(config["pix2pix"]["image_ext"]),
        )
        print(f"Exported Pix2Pix pairs: {result['total_pairs']}")
        print(f"Dataset folder: {result['out_dir']}")
        print(f"Pairs CSV: {result['pairs_csv']}")
        return

    # python -m app.main split-pix2pix
    if len(sys.argv) >= 2 and sys.argv[1] == "split-pix2pix":
        base_dir = Path(config["pix2pix"]["out_dir"])
        out_dir = Path(config["pix2pix_split"]["out_dir"])

        result = split_pix2pix_dataset(
            base_dir=base_dir,
            out_dir=out_dir,
            train_ratio=float(config["pix2pix_split"]["train_ratio"]),
            val_ratio=float(config["pix2pix_split"]["val_ratio"]),
            test_ratio=float(config["pix2pix_split"]["test_ratio"]),
            seed=int(config["pix2pix_split"]["seed"]),
        )
        print(f"Split done: train={result['train']} val={result['val']} test={result['test']}")
        print(f"Split folder: {result['out_dir']}")
        print(f"Split CSV: {result['split_csv']}")
        return

    # python -m app.main train-pix2pix
    # python -m app.main train-pix2pix
    if len(sys.argv) >= 2 and sys.argv[1] == "train-pix2pix":
        data_root = Path(config["pix2pix_split"]["out_dir"])

        # папка для результатов
        out_dir = Path("data/pix2pix_runs/run_cpu_1")

        train_pix2pix(
            data_root=data_root,
            out_dir=out_dir,
            epochs=15,  # CPU: сначала 15 эпох
            batch_size=1,  # CPU: обязательно 1
            lr=2e-4,
            lambda_l1=100.0,
            device="cpu",  # явно CPU
        )
        print(f"Training done. See: {out_dir}")
        return

    # python -m app.main eval-pix2pix
    if len(sys.argv) >= 2 and sys.argv[1] == "eval-pix2pix":
        data_root = Path(config["pix2pix_split"]["out_dir"])
        ckpt = Path("data/pix2pix_runs/run_cpu_1/checkpoints/G_epoch_015.pt")
        out_dir = Path("data/pix2pix_runs/run_cpu_1/eval_test")

        mae_mean, psnr_mean = eval_pix2pix(
            data_root=data_root,
            checkpoint_path=ckpt,
            out_dir=out_dir,
            device="cpu",
        )
        print(f"Eval done. MAE={mae_mean:.6f} PSNR={psnr_mean:.3f}")
        print(f"Results folder: {out_dir}")
        return

    # python -m app.main make-preview-from-checkpoint
    if len(sys.argv) >= 2 and sys.argv[1] == "make-preview-from-checkpoint":
        from app.gan.infer_from_checkpoint import make_previews_from_checkpoint

        pix2pix_split_dir = Path("data/pix2pix_split")

        checkpoint_path = Path("data/pix2pix_runs/run_cpu_1/checkpoints/G_epoch_015.pt")

        out_dir = Path("data/pix2pix_runs/run_cpu_1/infer_test_20")

        num_samples = 20

        if len(sys.argv) >= 3:
            checkpoint_path = Path(sys.argv[2])
        if len(sys.argv) >= 4:
            num_samples = int(sys.argv[3])

        saved = make_previews_from_checkpoint(
            pix2pix_split_dir=pix2pix_split_dir,
            checkpoint_path=checkpoint_path,
            out_dir=out_dir,
            num_samples=num_samples,
            device="cpu",
            image_size=256,
        )
        print(f"Infer done. Saved {saved} previews to: {out_dir}")
        return

    # python -m app.main apply-pix2pix <tree_id> <target_h> [--src-h <src_h>] [--allow-far] [--overwrite-real] [--all-missing]
    if len(sys.argv) >= 2 and sys.argv[1] == "apply-pix2pix":
        from app.db.connection import get_connection

        tree_id = None
        target_h = None
        src_h = None

        all_missing = False
        allow_far = False
        overwrite_real = False
        allow_synth_as_source = False

        # Примеры:
        # python -m app.main apply-pix2pix tree_001 55
        # python -m app.main apply-pix2pix tree_001 65 --src-h 50
        # python -m app.main apply-pix2pix tree_001 65 --allow-far
        # python -m app.main apply-pix2pix tree_001 --all-missing --allow-far

        if len(sys.argv) >= 3:
            tree_id = sys.argv[2]

        if len(sys.argv) >= 4 and not sys.argv[3].startswith("--"):
            target_h = float(sys.argv[3])

        if "--src-h" in sys.argv:
            i = sys.argv.index("--src-h")
            src_h = float(sys.argv[i + 1])

        if "--all-missing" in sys.argv:
            all_missing = True

        if "--allow-far" in sys.argv:
            allow_far = True

        if "--overwrite-real" in sys.argv:
            overwrite_real = True

        if "--allow-synth-source" in sys.argv:
            allow_synth_as_source = True

        if not tree_id:
            print("Usage:")
            print("  python -m app.main apply-pix2pix <tree_id> <target_h>")
            print("  python -m app.main apply-pix2pix <tree_id> <target_h> --src-h <src_h>")
            print("  python -m app.main apply-pix2pix <tree_id> <target_h> --allow-far")
            print("  python -m app.main apply-pix2pix <tree_id> --all-missing --allow-far")
            return

        levels_grid = [float(x) for x in config["heights_grid"]["levels_m"]]
        checkpoint_path = Path("data/pix2pix_runs/run_cpu_1/checkpoints/G_epoch_015.pt")
        roi_norm_dir = Path(config["paths"]["roi_norm_dir"])

        # ===== РЕЖИМ: заполнить все пустые уровни =====
        if all_missing:
            missing = []

            with get_connection(db_path) as conn:
                cur = conn.cursor()

                for h in levels_grid:
                    cur.execute(
                        """
                        SELECT data_type, roi_norm_path
                        FROM crown_levels
                        WHERE tree_id = ?
                          AND h_level = ? LIMIT 1
                        """,
                        (tree_id, float(h)),
                    )
                    r = cur.fetchone()

                    # пустой уровень:
                    # - строки нет
                    # - или roi_norm_path отсутствует
                    if r is None or r["roi_norm_path"] is None:
                        missing.append(float(h))

            created = 0

            for h in missing:
                outp = apply_pix2pix_one(
                    db_path=db_path,
                    tree_id=tree_id,
                    target_h=h,
                    levels_grid=levels_grid,
                    checkpoint_path=checkpoint_path,
                    roi_norm_dir=roi_norm_dir,
                    src_h=None,
                    device="cpu",
                    allow_synth_as_source=allow_synth_as_source,
                    allow_far=allow_far,
                    overwrite_real=overwrite_real,
                )

                if outp is not None:
                    created += 1

            print(f"Pix2Pix all-missing done. Created {created} synth levels.")
            return

        # ===== РЕЖИМ: один уровень =====
        if target_h is None:
            print("Usage:")
            print("  python -m app.main apply-pix2pix <tree_id> <target_h>")
            print("  python -m app.main apply-pix2pix <tree_id> <target_h> --src-h <src_h>")
            return

        outp = apply_pix2pix_one(
            db_path=db_path,
            tree_id=tree_id,
            target_h=target_h,
            levels_grid=levels_grid,
            checkpoint_path=checkpoint_path,
            roi_norm_dir=roi_norm_dir,
            src_h=src_h,
            device="cpu",
            allow_synth_as_source=allow_synth_as_source,
            allow_far=allow_far,
            overwrite_real=overwrite_real,
        )

        print(f"Done. Output: {outp}")
        return

    # python -m app.main migrate-levels-pix2pix
    if len(sys.argv) >= 2 and sys.argv[1] == "migrate-levels-pix2pix":
        migrate_crown_levels_for_pix2pix(db_path=db_path)
        print("Migration done: crown_levels columns for pix2pix are ready.")
        return

    # python -m app.main restore-real-level <tree_id> <h_level> <roi_norm_path>
    if len(sys.argv) >= 2 and sys.argv[1] == "restore-real-level":
        if len(sys.argv) < 5:
            print("Usage: python -m app.main restore-real-level <tree_id> <h_level> <roi_norm_path>")
            print("Example: python -m app.main restore-real-level tree_001 50 data\\roi_norm\\tree_001_50.png")
            return

        tree_id = sys.argv[2]
        h_level = float(sys.argv[3])
        roi_norm_path = sys.argv[4]

        from app.db.connection import get_connection

        with get_connection(db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE crown_levels
                SET data_type = ?,
                    roi_norm_path = ?,
                    synth_method = NULL,
                    synth_src_h = NULL,
                    mapping_error = 0.0
                WHERE tree_id = ?
                  AND h_level = ?
                """,
                (
                    "REAL",
                    roi_norm_path,
                    tree_id,
                    h_level,
                ),
            )
            conn.commit()

        print(f"Restored REAL level: tree_id={tree_id}, h_level={h_level}, roi={roi_norm_path}")
        return

    # ===== ЕСЛИ БЕЗ АРГУМЕНТОВ =====
    print("\nRun modes:")
    print("  python -m app.main import")
    print("  python -m app.main list-images")
    print("  python -m app.main annotate <tree_id> <tree_type>")
    print("  python -m app.main list-annotations")
    print("  python -m app.main build-observations")
    print("  python -m app.main dedup-annotations")
    print("  python -m app.main cleanup-observations")
    print("  python -m app.main list-observations")
    print("  python -m app.main show-observation <obs_id>")
    print("  python -m app.main backfill-obs-height")
    print("  python -m app.main check-heights")
    print("  python -m app.main fill-flight-altitude-from-filename")
    print("  python -m app.main build-levels")
    print("  python -m app.main show-levels <tree_id>")
    print("  python -m app.main normalize-scale")
    print("  python -m app.main synthesize-missing [tree_id] [level]")
    print("  python -m app.main export-dataset-pairs [only_tree_id]")
    print("  python -m app.main export-pix2pix")
    print("  python -m app.main split-pix2pix")
    print("  python -m app.main train-pix2pix")
    print("  python -m app.main eval-pix2pix")
    print("  python -m app.main make-preview-from-checkpoint")
    print("  python -m app.main apply-pix2pix <tree_id> <target_h>")
    print("  python -m app.main migrate-levels-pix2pix")
if __name__ == "__main__":
    main()
