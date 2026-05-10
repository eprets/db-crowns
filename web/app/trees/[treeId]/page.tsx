type LevelRow = {
  h_level: number;
  data_type: string;
  roi_norm_path: string | null;
  roi_mask_norm_path: string | null;
  synth_method: string | null;
  synth_src_h: number | null;
};

type LevelsResponse = {
  tree_id: string;
  levels: LevelRow[];
};

async function getLevels(treeId: string): Promise<LevelsResponse> {
  const res = await fetch(`http://127.0.0.1:8000/trees/${treeId}/levels`, {
    cache: "no-store",
  });

  if (!res.ok) {
    throw new Error("Failed to load levels");
  }

  return res.json();
}

export default async function TreePage({
  params,
}: {
  params: Promise<{ treeId: string }>;
}) {
  const { treeId } = await params;
  const data = await getLevels(treeId);

  const previewUrl = `http://127.0.0.1:8000/trees/${treeId}/preview`;

  const totalLevels = data.levels.length;
  const realCount = data.levels.filter(
    (level) => String(level.data_type).toUpperCase() === "REAL"
  ).length;
  const synthCount = data.levels.filter(
    (level) => String(level.data_type).toUpperCase() === "SYNTH"
  ).length;
  const roiCount = data.levels.filter((level) => level.roi_norm_path).length;
  const maskCount = data.levels.filter((level) => level.roi_mask_norm_path).length;

  const missingRoi = data.levels.filter((level) => !level.roi_norm_path);
  const missingMasks = data.levels.filter((level) => !level.roi_mask_norm_path);

  const realPercent = Math.round((realCount / totalLevels) * 100);
  const synthPercent = Math.round((synthCount / totalLevels) * 100);
  const roiPercent = Math.round((roiCount / totalLevels) * 100);
  const maskPercent = Math.round((maskCount / totalLevels) * 100);

  return (
    <main className="min-h-screen bg-black text-zinc-100">
      <section className="mx-auto max-w-7xl px-6 py-10">
        <a
          href="/"
          className="mb-8 inline-flex text-sm text-emerald-400 hover:text-emerald-300"
        >
          ← Назад
        </a>

        <div className="mb-10 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="mb-2 text-sm uppercase tracking-[0.3em] text-emerald-400">
              Tree profile
            </p>

            <h1 className="text-5xl font-bold">{treeId}</h1>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <a
              href={`http://127.0.0.1:8000/trees/${treeId}/profile-csv`}
              className="rounded-2xl border border-emerald-500/40 bg-emerald-500/10 px-5 py-3 text-sm font-medium text-emerald-300 transition hover:bg-emerald-500/20"
            >
              Скачать CSV
            </a>

            <a
              href={`http://127.0.0.1:8000/trees/${treeId}/profile-zip`}
              className="rounded-2xl border border-sky-500/40 bg-sky-500/10 px-5 py-3 text-sm font-medium text-sky-300 transition hover:bg-sky-500/20"
            >
              Скачать ZIP
            </a>

            <div className="rounded-2xl border border-zinc-800 bg-zinc-900 px-5 py-3">
              <p className="text-sm text-zinc-400">
                Levels: {data.levels.length}
              </p>
            </div>
          </div>
        </div>

        <div className="mb-10 grid gap-5 md:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-5">
            <p className="text-sm text-zinc-500">REAL уровни</p>
            <p className="mt-2 text-4xl font-bold text-emerald-300">{realCount}</p>
            <div className="mt-4 h-2 overflow-hidden rounded-full bg-zinc-800">
              <div className="h-full rounded-full bg-emerald-400" style={{ width: `${realPercent}%` }} />
            </div>
            <p className="mt-2 text-xs text-zinc-500">{realPercent}% от всех уровней</p>
          </div>

          <div className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-5">
            <p className="text-sm text-zinc-500">SYNTH уровни</p>
            <p className="mt-2 text-4xl font-bold text-orange-300">{synthCount}</p>
            <div className="mt-4 h-2 overflow-hidden rounded-full bg-zinc-800">
              <div className="h-full rounded-full bg-orange-400" style={{ width: `${synthPercent}%` }} />
            </div>
            <p className="mt-2 text-xs text-zinc-500">{synthPercent}% восстановлено синтезом</p>
          </div>

          <div className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-5">
            <p className="text-sm text-zinc-500">ROI покрытие</p>
            <p className="mt-2 text-4xl font-bold text-sky-300">
              {roiCount}/{totalLevels}
            </p>
            <div className="mt-4 h-2 overflow-hidden rounded-full bg-zinc-800">
              <div className="h-full rounded-full bg-sky-400" style={{ width: `${roiPercent}%` }} />
            </div>
            <p className="mt-2 text-xs text-zinc-500">{roiPercent}% уровней имеют ROI</p>
          </div>

          <div className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-5">
            <p className="text-sm text-zinc-500">Маски</p>
            <p className="mt-2 text-4xl font-bold text-violet-300">
              {maskCount}/{totalLevels}
            </p>
            <div className="mt-4 h-2 overflow-hidden rounded-full bg-zinc-800">
              <div className="h-full rounded-full bg-violet-400" style={{ width: `${maskPercent}%` }} />
            </div>
            <p className="mt-2 text-xs text-zinc-500">{maskPercent}% уровней имеют маску</p>
          </div>
        </div>

        <div className="mb-10 grid gap-5 lg:grid-cols-2">
          <div className="rounded-3xl border border-zinc-800 bg-zinc-900/70 p-5">
            <h2 className="mb-4 text-xl font-semibold">Покрытие высот</h2>

            <div className="flex flex-wrap gap-2">
              {data.levels.map((level) => {
                const isReal = String(level.data_type).toUpperCase() === "REAL";

                return (
                  <span
                    key={level.h_level}
                    className={`rounded-full px-3 py-1 text-sm ${
                      isReal
                        ? "bg-emerald-500/20 text-emerald-300"
                        : "bg-orange-500/20 text-orange-300"
                    }`}
                  >
                    {level.h_level}m {isReal ? "REAL" : "SYNTH"}
                  </span>
                );
              })}
            </div>
          </div>

          <div className="rounded-3xl border border-zinc-800 bg-zinc-900/70 p-5">
            <h2 className="mb-4 text-xl font-semibold">Проверка данных</h2>

            <div className="space-y-3 text-sm">
              <div className="flex justify-between border-b border-zinc-800 pb-2">
                <span className="text-zinc-400">Пропуски ROI</span>
                <span className={missingRoi.length ? "text-red-300" : "text-emerald-300"}>
                  {missingRoi.length ? missingRoi.map((x) => `${x.h_level}m`).join(", ") : "нет"}
                </span>
              </div>

              <div className="flex justify-between border-b border-zinc-800 pb-2">
                <span className="text-zinc-400">Пропуски масок</span>
                <span className={missingMasks.length ? "text-red-300" : "text-emerald-300"}>
                  {missingMasks.length ? missingMasks.map((x) => `${x.h_level}m`).join(", ") : "нет"}
                </span>
              </div>

              <div className="flex justify-between">
                <span className="text-zinc-400">Состояние профиля</span>
                <span className="text-emerald-300">
                  {missingRoi.length === 0 && missingMasks.length === 0 ? "полный" : "требует проверки"}
                </span>
              </div>
            </div>
          </div>
        </div>

        <div className="mb-10 overflow-hidden rounded-3xl border border-zinc-800 bg-zinc-950">
          <img src={previewUrl} alt={treeId} className="w-full" />
        </div>

        <div className="mb-10">
          <h2 className="mb-5 text-2xl font-semibold">Уровни дерева</h2>

          <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {data.levels.map((level) => {
              const roiUrl = level.roi_norm_path
                ? `http://127.0.0.1:8000/file?path=${encodeURIComponent(level.roi_norm_path)}`
                : null;

              const maskUrl = level.roi_mask_norm_path
                ? `http://127.0.0.1:8000/file?path=${encodeURIComponent(level.roi_mask_norm_path)}`
                : null;

              const isReal = String(level.data_type).toUpperCase() === "REAL";

              return (
                <div
                  key={level.h_level}
                  className="overflow-hidden rounded-2xl border border-zinc-800 bg-zinc-900/80"
                >
                  <div className="flex items-center justify-between border-b border-zinc-800 px-4 py-3">
                    <div>
                      <p className="text-lg font-semibold">{level.h_level} m</p>
                      <p className="text-xs text-zinc-500">
                        {level.synth_method
                          ? `${level.synth_method} from ${level.synth_src_h ?? "-"}`
                          : "observed"}
                      </p>
                    </div>

                    <span
                      className={`rounded-full px-3 py-1 text-xs font-semibold ${
                        isReal
                          ? "bg-emerald-500/20 text-emerald-300"
                          : "bg-orange-500/20 text-orange-300"
                      }`}
                    >
                      {level.data_type}
                    </span>
                  </div>

                  <div className="grid grid-cols-2 gap-px bg-zinc-800">
                    <div className="bg-black">
                      <div className="border-b border-zinc-800 px-3 py-2 text-xs uppercase tracking-wider text-zinc-500">
                        ROI
                      </div>

                      {roiUrl ? (
                        <img src={roiUrl} alt={`${treeId} ${level.h_level} ROI`} className="aspect-square w-full object-cover" />
                      ) : (
                        <div className="flex aspect-square items-center justify-center text-sm text-zinc-600">
                          no roi
                        </div>
                      )}
                    </div>

                    <div className="bg-black">
                      <div className="border-b border-zinc-800 px-3 py-2 text-xs uppercase tracking-wider text-zinc-500">
                        Mask
                      </div>

                      {maskUrl ? (
                        <img src={maskUrl} alt={`${treeId} ${level.h_level} mask`} className="aspect-square w-full object-cover" />
                      ) : (
                        <div className="flex aspect-square items-center justify-center text-sm text-zinc-600">
                          no mask
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="overflow-hidden rounded-3xl border border-zinc-800">
          <table className="w-full border-collapse">
            <thead className="bg-zinc-900">
              <tr className="text-left text-sm text-zinc-400">
                <th className="px-4 py-4">Height</th>
                <th className="px-4 py-4">Type</th>
                <th className="px-4 py-4">ROI</th>
                <th className="px-4 py-4">Mask</th>
                <th className="px-4 py-4">Method</th>
                <th className="px-4 py-4">Source H</th>
              </tr>
            </thead>

            <tbody>
              {data.levels.map((level) => {
                const isReal = String(level.data_type).toUpperCase() === "REAL";

                return (
                  <tr key={level.h_level} className="border-t border-zinc-800 bg-zinc-950">
                    <td className="px-4 py-4 font-medium">{level.h_level} m</td>

                    <td className="px-4 py-4">
                      <span
                        className={`rounded-full px-3 py-1 text-xs font-semibold ${
                          isReal
                            ? "bg-emerald-500/20 text-emerald-300"
                            : "bg-orange-500/20 text-orange-300"
                        }`}
                      >
                        {level.data_type}
                      </span>
                    </td>

                    <td className="px-4 py-4">{level.roi_norm_path ? "YES" : "NO"}</td>
                    <td className="px-4 py-4">{level.roi_mask_norm_path ? "YES" : "NO"}</td>
                    <td className="px-4 py-4 text-zinc-400">{level.synth_method ?? "-"}</td>
                    <td className="px-4 py-4 text-zinc-400">{level.synth_src_h ?? "-"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}