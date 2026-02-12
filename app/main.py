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
from app.backfill_obs_height import backfill_obs_height_from_images
from app.check_heights import print_heights_summary
from app.fill_flight_altitude import fill_flight_altitude_from_filename
from app.build_levels import build_levels, show_levels


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
    if len(sys.argv) >= 2 and sys.argv[1] == "backfill-obs-height":
        updated = backfill_obs_height_from_images(db_path)
        print(f"Backfill done. Updated {updated} observations.")
        return

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



if __name__ == "__main__":
    main()
