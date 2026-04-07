# Algorithmus Prime

Sistema multi-agente para orquestación de pipelines inteligentes.

## Arquitectura

El sistema está compuesto por 4 agentes principales:

- **Orchestrator** → Motor de decisión central
- **Cronista** → Registro y estructuración de información
- **Guardia** → Validación y control
- **Velocista** → Ejecución de tareas rápidas

## Estructura

```
services/
  orchestrator/
  cronista/
  guardia/
  velocista/

shared/
  contracts/

infra/
```

## Conceptos clave

- `execution_id` → identifica una ejecución completa
- `correlation_id` → trazabilidad entre eventos
- event-driven pipeline
- ejecución paralela controlada

## Objetivo

Construir un sistema escalable de agentes inteligentes capaz de:

- procesar pipelines complejos
- tomar decisiones distribuidas
- integrarse con APIs externas
