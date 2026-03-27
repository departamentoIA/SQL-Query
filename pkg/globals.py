from __future__ import annotations
from pathlib import Path

# Límite práctico solicitado por el usuario.
MAX_ROWS_PER_XLSX_FILE = 1_000_000
DEFAULT_FETCH_BATCH_SIZE = 100_000
DEFAULT_COMMAND_TIMEOUT = 0  # 0 = sin límite en pyodbc
OUTPUT_ROOT = Path("output")
# RFC_list = ['CIE800912J23']

# Define aquí todos los jobs que quieras ejecutar de forma consecutiva.
# Cada job usa la MISMA conexión/sesión durante sus sentencias, por lo que
# puedes crear tablas temporales (#A, #B, etc.) antes del SELECT final.
QUERIES = [
    {
        "name": "Emisor_CIE800912J23",
        "output_dir": str(OUTPUT_ROOT),
        "sheet_name": "Emisor",
        "setup_sql": [
            """
            IF OBJECT_ID('tempdb..#E') IS NOT NULL
                DROP TABLE #E;
            """,
            """
            SELECT  TOP (1) *
            INTO #E
            FROM [SAT_V2].[dbo].[ANEXO_1A_2025_FULL]
            WHERE [EmisorRFC] = 'CIE800912J23';
            """,
            """
            CREATE CLUSTERED INDEX IX_E_UUID ON #E(UUID);
            """,
        ],
        "final_sql": """
            SELECT
                E.*,
                B.[ConceptoNoIdentificacion],
                B.[ConceptoClaveProdServ],
                B.[ConceptoCantidad],
                B.[ConceptoUnidad],
                B.[ConceptoDescripcion],
                B.[ConceptoValorUnitario],
                B.[ConceptoImporte]
            FROM #E AS E
            LEFT JOIN [SAT_V2].[dbo].[ANEXO_2B_2025_FULL] AS B
                ON E.[UUID] = B.[UUID];
        """,
    },
    {
        "name": "Receptor_CIE800912J23",
        "output_dir": str(OUTPUT_ROOT),
        "sheet_name": "Receptor",
        "setup_sql": [
            """
            IF OBJECT_ID('tempdb..#R') IS NOT NULL
                DROP TABLE #R;
            """,
            """
            SELECT  TOP (1) *
            INTO #R
            FROM [SAT_V2].[dbo].[ANEXO_1A_2025_FULL]
            WHERE [ReceptorRFC] = 'CIE800912J23';
            """,
            """
            CREATE CLUSTERED INDEX IX_R_UUID ON #R(UUID);
            """,
        ],
        "final_sql": """
            SELECT
                R.*,
                B.[ConceptoNoIdentificacion],
                B.[ConceptoClaveProdServ],
                B.[ConceptoCantidad],
                B.[ConceptoUnidad],
                B.[ConceptoDescripcion],
                B.[ConceptoValorUnitario],
                B.[ConceptoImporte]
            FROM #R AS R
            LEFT JOIN [SAT_V2].[dbo].[ANEXO_2B_2025_FULL] AS B
                ON R.[UUID] = B.[UUID];
        """,
    },
]
