"""Ejecuta el pipeline sobre data/payments.jsonl y muestra la evidencia.

Carga las funciones directamente desde notebook.py con el mismo mecanismo que
usa tests/conftest.py, de modo que no se duplica la solucion.

Uso:
    uv run python evidencia/run_demo.py
"""

from __future__ import annotations

import ast
import json
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer

ROOT = Path(__file__).parents[1]
NOTEBOOK_PATH = ROOT / "notebook.py"
DATA_PATH = ROOT / "data" / "payments.jsonl"

DEFINITIONS = {
    "parse_utc",
    "assign_fixed_window",
    "summarize_payments",
    "build_windowed_totals_pipeline",
    "DeduplicatePayments",
    "build_trigger_policy",
    "make_idempotency_key",
    "simulate_sink_retries",
}


def load_solution() -> SimpleNamespace:
    tree = ast.parse(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    nodes: list[ast.AST] = []
    for cell in tree.body:
        if not isinstance(cell, ast.FunctionDef) or cell.name != "_":
            continue
        for stmt in cell.body:
            if (
                isinstance(stmt, (ast.FunctionDef, ast.ClassDef))
                and stmt.name in DEFINITIONS
            ):
                nodes.append(stmt)
    namespace: dict[str, Any] = {
        "Any": Any,
        "Iterable": Iterable,
        "SetStateSpec": SetStateSpec,
        "StrUtf8Coder": StrUtf8Coder,
        "TimeDomain": TimeDomain,
        "TimerSpec": TimerSpec,
        "beam": beam,
        "datetime": datetime,
        "on_timer": on_timer,
    }
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, str(NOTEBOOK_PATH), "exec"), namespace)
    return SimpleNamespace(**{name: namespace[name] for name in DEFINITIONS})


def load_events() -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in DATA_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def main() -> None:
    solution = load_solution()
    events = load_events()

    print("=" * 70)
    print("EVIDENCIA DE EJECUCION - Tarea 3")
    print("=" * 70)
    print(f"eventos de entrada: {len(events)}")

    totals, audit = solution.summarize_payments(events)
    aceptados = [row for row in audit if row["accepted"]]
    print(f"eventos aceptados : {len(aceptados)}")
    print(f"totales producidos: {len(totals)}")

    print("\n--- Auditoria por evento ---")
    header = f"{'event_id':9} {'merchant':8} {'delay':>6} {'acc':>4} {'dup':>4} {'late':>5} {'rev':>4} reason"
    print(header)
    for row in audit:
        print(
            f"{row['event_id']:9} {row['merchant_id']:8} "
            f"{row['delay_seconds']:>6} {str(row['accepted']):>4} "
            f"{str(row['duplicate']):>4} {str(row['too_late']):>5} "
            f"{str(row['revision']):>4} {row['reason']}"
        )

    print("\n--- Totales confirmados por comercio y ventana ---")
    for row in sorted(totals, key=lambda r: (r["merchant_id"], r["window_start"])):
        print(
            f"{row['merchant_id']:8} {row['window_start']} .. "
            f"{row['window_end']}  total={row['total']}"
        )

    print("\n--- Pipeline Beam windowed (misma entrada CONFIRMED) ---")
    with TestPipeline() as pipeline:
        result = solution.build_windowed_totals_pipeline(
            pipeline, events, window_seconds=60
        )
        result | "Imprimir" >> beam.Map(print)

    print("\n--- Reintentos de sink idempotente vs append-only ---")
    idem_mat, idem_audit = solution.simulate_sink_retries(
        totals, attempts=2, idempotent=True
    )
    post_mat, post_audit = solution.simulate_sink_retries(
        totals, attempts=2, idempotent=False
    )
    print(
        f"UPSERT: {len(idem_audit)} intentos -> "
        f"{len(idem_mat)} filas materializadas"
    )
    print(
        f"POST  : {len(post_audit)} intentos -> "
        f"{len(post_mat)} filas materializadas"
    )


if __name__ == "__main__":
    main()
