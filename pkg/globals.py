from __future__ import annotations

from pathlib import Path

# Límite práctico solicitado por el usuario.
MAX_ROWS_PER_XLSX_FILE = 1_000_000
DEFAULT_FETCH_BATCH_SIZE = 100_000
DEFAULT_COMMAND_TIMEOUT = 0  # 0 = sin límite en pyodbc
OUTPUT_ROOT = Path("output")

# Define aquí todos los jobs que quieras ejecutar de forma consecutiva.
# Cada job usa la MISMA conexión/sesión durante sus sentencias, por lo que
# puedes crear tablas temporales (#A, #B, etc.) antes del SELECT final.
QUERIES = [
    {
        "name": "anexo_rfc_nhb770831bw3",
        "output_dir": str(OUTPUT_ROOT),
        "sheet_name": "resultado",
        "setup_sql": [
            """
            IF OBJECT_ID('tempdb..#A') IS NOT NULL
                DROP TABLE #A;
            """,
            """
            SELECT *
            INTO #A
            FROM [SAT_V2].[dbo].[ANEXO_1A_2025_FULL]
            WHERE [EmisorRFC] = 'NHB770831BW3';
            """,
            """
            CREATE CLUSTERED INDEX IX_A_UUID ON #A(UUID);
            """,
        ],
        "final_sql": """
            SELECT
                A.*,
                B.[ConceptoNoIdentificacion],
                B.[ConceptoClaveProdServ],
                B.[ConceptoCantidad],
                B.[ConceptoUnidad],
                B.[ConceptoDescripcion],
                B.[ConceptoValorUnitario],
                B.[ConceptoImporte]
            FROM #A AS A
            LEFT JOIN [SAT_V2].[dbo].[ANEXO_2B_2025_FULL] AS B
                ON A.[UUID] = B.[UUID];
        """,
    },
]
