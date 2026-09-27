# Tarea 3 — Beam avanzado

Proyecto base autocontenido para la asignatura **Streaming de datos y sus
aplicaciones**. La tarea consiste en completar un pipeline de pagos con tiempo
de evento, ventanas, estado por clave y una salida idempotente.

El repositorio es deliberadamente un esqueleto: `notebook.py` contiene la
consigna, contratos y funciones sin implementación. No incluye la solución.

## Objetivo

Producir totales confirmados por comercio y minuto:

- usando `event_time`, no el tiempo de llegada;
- tolerando hasta 120 segundos de atraso;
- descartando estados distintos de `CONFIRMED`;
- deduplicando `event_id` dentro de cada comercio;
- conservando metadatos de ventana y pane;
- materializando la salida mediante una clave idempotente.

## Ejecutar con Docker

Desde este directorio:

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. Docker inicia Marimo en modo editor porque la
tarea requiere completar las celdas de código. Los cambios en `notebook.py` se
guardan en el directorio local.

El editor usa `--no-token` para simplificar el trabajo en `localhost`; no debe
exponerse directamente a una red pública.

## Ejecutar con uv

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

## Trabajar con tests

```bash
uv run pytest
```

Los tests se entregan deliberadamente en rojo: las funciones del notebook
lanzan `NotImplementedError`. El objetivo es implementar las celdas hasta
obtener una suite completamente verde.

Los tests cargan las funciones directamente desde `notebook.py`; no hay que
copiar la solución a otro módulo.

Para validar además estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Dentro del contenedor también se puede ejecutar:

```bash
docker compose exec notebook uv run pytest
```

## Entrega

Entregar un repositorio propio que incluya:

- `notebook.py` con todas las funciones implementadas;
- evidencia de ejecución del pipeline;
- todas las pruebas provistas para desorden, duplicados, atraso y reintentos
  ejecutadas y aprobadas;
- un README breve con decisiones y trade-offs;
- instrucciones reproducibles con Docker o `uv`.

No modificar `data/payments.jsonl`; puede agregarse un conjunto de datos
adicional para las pruebas.

---

## Estado de la entrega

Suite completa en verde: 13 de 13 pruebas provistas.

```
13 passed in 2.28s
```

La salida detallada de `pytest -v` esta en `evidencia/salida-pytest.txt` y la
ejecucion del pipeline sobre `data/payments.jsonl` en
`evidencia/salida-pipeline.txt`. Para regenerarlas:

```bash
uv run pytest -v
uv run python evidencia/run_demo.py
```

## Decisiones y trade-offs

### Contrato temporal y ventanas

- `parse_utc` normaliza cualquier timestamp ISO-8601 a UTC timezone-aware.
  Acepta el sufijo `Z` (lo convierte a `+00:00`) y rechaza valores vacios o
  malformados con `ValueError`. Trabajar siempre en UTC evita errores de zona
  horaria al comparar tiempos de eventos de distintas fuentes.
- `assign_fixed_window` calcula los limites `[inicio, fin)` alineando el
  tiempo de evento a la grilla de ventanas fijas (60 s por defecto). Se usa el
  tiempo de evento, no el de llegada, para que un pago cuente en el minuto en
  que ocurrio aunque llegue tarde.
- La ventana es fija de 60 segundos con intervalos semiabiertos: un evento con
  tiempo exactamente en el borde pertenece a la ventana siguiente.

### Estado, deduplicacion y expiracion

- `summarize_payments` es un oraculo determinista en Python puro que fija el
  resultado esperado antes de Beam: solo suma `CONFIRMED`, deduplica por
  `(merchant_id, event_id)`, calcula el atraso como `arrival - event`, y marca
  cada evento en la auditoria con su razon (`accepted`, `duplicate`,
  `too_late`, `status`). Un late aceptado queda con `revision=True`.
- `DeduplicatePayments` lleva la deduplicacion a Beam con estado por clave
  (`SetStateSpec`). El estado se particiona por comercio, asi que un mismo
  `event_id` en dos comercios distintos no colisiona.
- El timer de event time (`TimerSpec` en dominio WATERMARK) limpia el estado
  cuando el watermark supera el fin de la ventana. Sin esa expiracion, el
  conjunto de `event_id` vistos crece de forma indefinida: cada clave
  acumularia todos los identificadores para siempre y el estado no seria
  acotado.

### Idempotencia y reintentos

- `make_idempotency_key` construye `merchant_id|window_start`, que identifica
  de forma unica el resultado logico de una ventana.
- `simulate_sink_retries` contrasta dos contratos de escritura ante reintentos:
  el sink UPSERT (dict por clave idempotente) converge a una sola fila aunque
  se reintente; el sink POST append-only materializa una fila por intento. Con
  cuatro resultados y dos intentos, UPSERT deja 4 filas y POST deja 8. Esto
  muestra por que un efecto externo idempotente neutraliza los reintentos que
  Beam puede generar en at-least-once.

### Triggers

- `build_trigger_policy` arma una `WindowInto` con `AfterWatermark`: pane
  on-time cuando el watermark pasa el fin, estimaciones early cada 30 s de
  processing time, revisiones late por cada evento tardio, y modo
  ACCUMULATING para que cada pane reemplace al anterior con el total conocido.

### Diferencia entre el oraculo y el pipeline windowed base

`summarize_payments` aplica dedup y tolerancia de lateness, por eso para
m-verde en `13:00` reporta 80.000. El pipeline de `build_windowed_totals_pipeline`
es el paso de ventaneo crudo (Create, TimestampedValue, Filter, WindowInto,
CombinePerKey) sin dedup ni lateness, de modo que para la misma clave suma
todos los `CONFIRMED` de la ventana. La deduplicacion y la politica de lateness
se incorporan por separado con `DeduplicatePayments` y `build_trigger_policy`.

### Nota de compatibilidad de Apache Beam

La prueba `test_trigger_policy_has_lateness_and_accumulating_panes` accede a
`policy.windowing.windowfn.size.seconds` y `policy.windowing.allowed_lateness.seconds`.
En Apache Beam la clase `Duration` almacena el intervalo en microsegundos y no
expone un atributo `.seconds` (verificado en las versiones publicadas). Para que
la suite provista quede verde sin modificar los tests, `build_trigger_policy`
usa una subclase minima de `Duration` que agrega esa propiedad de solo lectura.
No cambia el comportamiento del windowing; solo lo hace inspeccionable en
segundos.
