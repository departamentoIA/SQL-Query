#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project:        SQL-Query
File:           main.py
Author:         Antonio Arteaga
Last Updated:   2026-03-19
Version:        1.0
Description:    
Dependencies:   polars==1.38.1, openpyxl==3.1.5, xlsxwriter==3.2.9,
Usage:          Permission is requested to connect to the DataBase.
"""
from pkg.globals import QUERIES
from pkg.extract import run_query_job
from pkg.exporter import export_dataframe_to_excel_files


def main() -> None:
    if not QUERIES:
        raise ValueError("No hay consultas definidas en pkg/globals.py")

    for job in QUERIES:
        print(f"[INFO] Ejecutando job: {job['name']}")
        df = run_query_job(job)
        generated_files = export_dataframe_to_excel_files(
            df=df,
            output_dir=job.get("output_dir", "output"),
            base_filename=job["name"],
            sheet_name=job.get("sheet_name", "data"),
        )

        print(f"[OK] Job finalizado: {job['name']}")
        print("Archivos generados:")
        for path in generated_files:
            print(f" - {path}")


if __name__ == "__main__":
    main()
