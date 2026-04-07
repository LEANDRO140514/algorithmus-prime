# Algorithmus Prime (ForgeClaw)

**Sistema multi-agente para la orquestación de pipelines inteligentes**, orientado a operar una agencia autónoma 24/7 con mínima intervención humana.

## Descripción general

Algorithmus Prime, también referido como **ForgeClaw**, funciona como un sistema operativo de agencia autónoma basado en OpenClaw + Clawe. Su propósito es coordinar agentes especializados para ejecutar flujos complejos, tomar decisiones distribuidas y mantener trazabilidad completa de cada ejecución.

## Arquitectura de agentes

El sistema está compuesto por 4 agentes principales:

- **Orchestrator (Orquestador):** motor central de decisión y coordinación.
- **Cronista:** registro, estructuración y continuidad de la información.
- **Guardia:** validación, control y cumplimiento de reglas.
- **Velocista:** ejecución de tareas rápidas y operativas.

## Conceptos clave

- `execution_id`: identifica una ejecución completa.
- `correlation_id`: permite la trazabilidad entre eventos y servicios.
- Pipeline orientado a eventos (`event-driven`).
- Ejecución paralela controlada entre agentes.

## Objetivos

Construir un sistema escalable de agentes inteligentes capaz de:

- Procesar pipelines complejos.
- Tomar decisiones distribuidas.
- Integrarse con APIs y servicios externos.

## Estructura del proyecto

```bash
algorithmus_prime/
├── docs/                  # Documentación (PRD, arquitectura, decisiones, roadmap)
├── infra/                 # OpenClaw, Clawe, Docker, n8n, Qdrant
├── services/              # Los 4 agentes core
│   ├── orchestrator/
│   ├── cronista/
│   ├── guardia/
│   └── velocista/
├── shared/                # Contratos, políticas y recursos compartidos
│   └── contracts/
├── .cursor/rules/         # Reglas operativas para Cursor/Composer
└── README.md
```

## Documentación relacionada

- `docs/PRD.md`
- `docs/tech-stack.md`
- `docs/architecture.md`
- `docs/decisions.md`
- `docs/roadmap.md`
