import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        from datetime import UTC

        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError(f"timestamp vacio o no textual: {raw_value!r}")

        text = raw_value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(
                f"timestamp ISO-8601 invalido: {raw_value!r}"
            ) from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        from datetime import UTC, timedelta

        aware = timestamp
        if aware.tzinfo is None:
            aware = aware.replace(tzinfo=UTC)

        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        elapsed = int((aware - epoch).total_seconds())
        start_seconds = (elapsed // size_seconds) * size_seconds

        start = epoch + timedelta(seconds=start_seconds)
        end = start + timedelta(seconds=size_seconds)
        return start, end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` contiene `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana son strings ISO-8601.

        Cada fila de `audit` contiene `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre on-time de su ventana.
        """
        totals: dict[tuple[str, str], dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for event in events:
            merchant_id = event["merchant_id"]
            event_id = event["event_id"]
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            delay_seconds = int((arrival_time - event_time).total_seconds())

            window_start, window_end = assign_fixed_window(
                event_time, window_seconds
            )
            key = (merchant_id, window_start.isoformat())
            dedup_key = (merchant_id, event_id)

            duplicate = False
            too_late = False
            accepted = False
            revision = False

            if event.get("status") != "CONFIRMED":
                reason = "status"
            elif delay_seconds > allowed_lateness_seconds:
                too_late = True
                reason = "too_late"
            elif deduplicate and dedup_key in seen:
                duplicate = True
                reason = "duplicate"
            else:
                accepted = True
                reason = "accepted"
                revision = arrival_time > window_end
                seen.add(dedup_key)
                bucket = totals.setdefault(
                    key,
                    {
                        "merchant_id": merchant_id,
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "total": 0,
                    },
                )
                bucket["total"] += event["amount"]

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        return list(totals.values()), audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usa Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        from datetime import UTC

        class _FormatTotal(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                merchant_id, total = element
                start = window.start.to_utc_datetime().replace(tzinfo=UTC)
                end = window.end.to_utc_datetime().replace(tzinfo=UTC)
                yield {
                    "merchant_id": merchant_id,
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "total": total,
                }

        def _to_timestamped(event):
            epoch = parse_utc(event["event_time"]).timestamp()
            pair = (event["merchant_id"], event["amount"])
            return beam.window.TimestampedValue(pair, epoch)

        return (
            pipeline
            | "Crear" >> beam.Create(events)
            | "SoloConfirmados"
            >> beam.Filter(lambda event: event.get("status") == "CONFIRMED")
            | "AsignarTiempoEvento" >> beam.Map(_to_timestamped)
            | "VentanaFija"
            >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "SumarPorComercio" >> beam.CombinePerKey(sum)
            | "Formatear" >> beam.ParDo(_FormatTotal())
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            _key, value = element
            event_id = value["event_id"]

            if event_id in set(seen_ids.read()):
                return

            seen_ids.add(event_id)
            expiry.set(window.end)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configura un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        from apache_beam.transforms.trigger import (
            AccumulationMode,
            AfterCount,
            AfterProcessingTime,
            AfterWatermark,
            Repeatedly,
        )
        from apache_beam.utils.timestamp import Duration

        class DurationConSegundos(Duration):
            """Duration que expone los segundos como atributo.

            Apache Beam almacena las duraciones en microsegundos y no ofrece un
            atributo `.seconds`; esta subclase lo agrega para que la política
            sea legible e inspeccionable en segundos.
            """

            @property
            def seconds(self) -> int:
                return self.micros // 1_000_000

        size = DurationConSegundos(window_seconds)
        lateness = DurationConSegundos(allowed_lateness_seconds)

        return beam.WindowInto(
            beam.window.FixedWindows(size),
            trigger=AfterWatermark(
                early=Repeatedly(AfterProcessingTime(30)),
                late=Repeatedly(AfterCount(1)),
            ),
            accumulation_mode=AccumulationMode.ACCUMULATING,
            allowed_lateness=lateness,
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado dejan una
        sola fila materializada. En modo append, cada intento agrega una.
        """
        upsert_sink: dict[str, dict[str, Any]] = {}
        append_sink: list[dict[str, Any]] = []
        audit: list[dict[str, Any]] = []
        operation = "UPSERT" if idempotent else "POST"

        for attempt in range(1, attempts + 1):
            for result in results:
                idempotency_key = make_idempotency_key(result)
                row = {
                    **result,
                    "idempotency_key": idempotency_key,
                    "attempt": attempt,
                    "operation": operation,
                }
                audit.append(row)
                if idempotent:
                    upsert_sink[idempotency_key] = row
                else:
                    append_sink.append(row)

        materialized = (
            list(upsert_sink.values()) if idempotent else append_sink
        )
        return materialized, audit

    return make_idempotency_key, simulate_sink_retries


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
