-- Таблица с изображениями и метаданными
CREATE TABLE IF NOT EXISTS images (
    image_id TEXT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,

    lat REAL,
    lon REAL,

    timestamp TEXT,
    day_of_year INTEGER,
    time_of_day TEXT,

    flight_altitude REAL,

    camera_model TEXT,
    focal_length REAL,

    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Таблица деревьев (объект исследования)
CREATE TABLE IF NOT EXISTS trees (
    tree_id TEXT PRIMARY KEY,
    tree_type TEXT,
    lat REAL,
    lon REAL,
    height_est REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Таблица аннотаций эллипсом (без масок, без CVAT)
CREATE TABLE IF NOT EXISTS annotations (
    annotation_id TEXT PRIMARY KEY,

    image_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    tree_type TEXT,

    x0 REAL NOT NULL,
    y0 REAL NOT NULL,
    a REAL NOT NULL,
    b REAL NOT NULL,
    theta REAL NOT NULL,

    quality REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (image_id) REFERENCES images(image_id),
    FOREIGN KEY (tree_id) REFERENCES trees(tree_id)
);

-- Индексы (ускоряют запросы)
CREATE INDEX IF NOT EXISTS idx_annotations_tree_id ON annotations(tree_id);
CREATE INDEX IF NOT EXISTS idx_annotations_image_id ON annotations(image_id);

-- Наблюдения кроны: ROI + признаки (V3)
CREATE TABLE IF NOT EXISTS crown_observations (
    obs_id TEXT PRIMARY KEY,

    annotation_id TEXT NOT NULL,
    image_id TEXT NOT NULL,
    tree_id TEXT NOT NULL,

    roi_raw_path TEXT NOT NULL,

    obs_height REAL
    features_json TEXT,       -- JSON строкой

    created_at TEXT DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (annotation_id) REFERENCES annotations(annotation_id),
    FOREIGN KEY (image_id) REFERENCES images(image_id),
    FOREIGN KEY (tree_id) REFERENCES trees(tree_id)
);

CREATE INDEX IF NOT EXISTS idx_obs_tree_id ON crown_observations(tree_id);
CREATE INDEX IF NOT EXISTS idx_obs_annotation_id ON crown_observations(annotation_id);

-- Защита от дублей: одна аннотация на (image_id, tree_id)
CREATE UNIQUE INDEX IF NOT EXISTS uq_annotations_image_tree
ON annotations(image_id, tree_id);
