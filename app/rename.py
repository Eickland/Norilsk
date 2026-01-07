import json
import shutil
from pathlib import Path
from typing import Any, Dict, Tuple, Set

import pandas as pd


JSON_PATH = Path("data/data.json")
XLSX_PATH = Path("temp/dick.xlsx")

# куда сохранять результат
OUT_JSON_PATH = JSON_PATH  # перезаписываем исходный файл
BACKUP_PATH = JSON_PATH.with_suffix(".json.bak")


def load_mapping(xlsx_path: Path) -> Dict[str, str]:
    df = pd.read_excel(xlsx_path)

    required = {"temp_name", "true_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В Excel нет нужных столбцов: {sorted(missing)}. Нужны: temp_name, true_name")

    # приводим к строкам, чистим пробелы; пустые/NaN игнорируем
    df = df.copy()
    df["temp_name"] = df["temp_name"].astype(str).str.strip()
    df["true_name"] = df["true_name"].astype(str).str.strip()

    # выкидываем строки, где temp_name пустой
    df = df[df["temp_name"].notna() & (df["temp_name"].str.len() > 0)]

    # если есть дубликаты temp_name — берём последнее вхождение
    mapping = dict(zip(df["temp_name"], df["true_name"]))
    return mapping


def replace_names(obj: Any, mapping: Dict[str, str]) -> Tuple[Any, int, Set[str]]:
    """
    Рекурсивно проходит по JSON-структуре и заменяет значения ключа "name",
    если значение есть в mapping.

    Возвращает:
      - изменённый объект
      - число замен
      - множество "name", которые встретились, но не были заменены
    """
    changes = 0
    not_changed: Set[str] = set()

    def _walk(x: Any) -> Any:
        nonlocal changes, not_changed

        if isinstance(x, dict):
            new_d = {}
            for k, v in x.items():
                if k == "name" and isinstance(v, str):
                    key = v.strip()
                    if key in mapping:
                        new_d[k] = mapping[key]
                        if mapping[key] != v:
                            changes += 1
                    else:
                        new_d[k] = v
                        not_changed.add(v)
                else:
                    new_d[k] = _walk(v)
            return new_d

        if isinstance(x, list):
            return [_walk(i) for i in x]

        return x

    return _walk(obj), changes, not_changed


def main() -> None:
    if not JSON_PATH.exists():
        raise FileNotFoundError(f"Не найден файл JSON: {JSON_PATH}")
    if not XLSX_PATH.exists():
        raise FileNotFoundError(f"Не найден файл Excel: {XLSX_PATH}")

    mapping = load_mapping(XLSX_PATH)

    with JSON_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    new_data, n_changes, not_changed = replace_names(data, mapping)

    # бэкап исходника
    shutil.copy2(JSON_PATH, BACKUP_PATH)

    # сохранить (красиво форматируем, чтобы было удобно смотреть diff)
    with OUT_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(new_data, f, ensure_ascii=False, indent=2)

    # вывод в терминал
    print(f"Готово. Замены выполнены: {n_changes}")
    print(f"Бэкап исходного файла: {BACKUP_PATH}")

    # полезно показать только те, что реально не нашлись в mapping
    # (и убрать пустые/None на всякий случай)
    not_changed = {x for x in not_changed if isinstance(x, str) and x.strip()}

    if not_changed:
        print("\nИмена, которые НЕ были изменены (нет в словаре):")
        for name in sorted(not_changed):
            print(f" - {name}")
    else:
        print("\nВсе встреченные значения 'name' были сопоставлены и обработаны.")


if __name__ == "__main__":
    main()
