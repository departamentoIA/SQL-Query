from __future__ import annotations

from pathlib import Path

import polars as pl

from pkg.globals import MAX_ROWS_PER_XLSX_FILE


def _sanitize_sheet_name(sheet_name: str) -> str:
    invalid_chars = [":", "\\", "/", "?", "*", "[", "]"]
    clean_name = sheet_name
    for char in invalid_chars:
        clean_name = clean_name.replace(char, "_")
    return clean_name[:31] or "data"


def export_dataframe_to_excel_files(
    df: pl.DataFrame,
    output_dir: str | Path,
    base_filename: str,
    sheet_name: str = "data",
    max_rows_per_file: int = MAX_ROWS_PER_XLSX_FILE,
) -> list[str]:
    """Exporta un DataFrame a uno o varios archivos XLSX."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    total_rows = df.height
    safe_sheet_name = _sanitize_sheet_name(sheet_name)

    if total_rows == 0:
        file_path = output_path / f"{base_filename}.xlsx"
        df.write_excel(workbook=str(file_path), worksheet=safe_sheet_name)
        return [str(file_path)]

    generated_files: list[str] = []
    start = 0
    part = 1

    while start < total_rows:
        chunk = df.slice(start, max_rows_per_file)
        suffix = f"_part_{part:03d}" if total_rows > max_rows_per_file else ""
        file_path = output_path / f"{base_filename}{suffix}.xlsx"

        chunk.write_excel(workbook=str(file_path), worksheet=safe_sheet_name)
        generated_files.append(str(file_path))

        print(
            "[INFO] Archivo exportado: "
            f"{file_path} ({chunk.height:,} filas)"
        )

        start += max_rows_per_file
        part += 1

    return generated_files
