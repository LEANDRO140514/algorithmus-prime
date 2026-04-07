# ForgeClaw

**Sistema Operativo de Agencia Autónoma** basado en OpenClaw + Clawe.

## Visión
ForgeClaw transforma un VPS de Hostinger en una agencia digital que funciona 24/7 con mínima intervención humana, mediante 4 agentes especializados: **Orquestador, Cronista, Guardia y Velocista**.

## Estructura del Proyecto

```bash
ForgeClaw/
├── docs/                  # Documentación (PRD, Tech Stack, etc.)
├── infra/                 # OpenClaw, Clawe, Docker, n8n, Qdrant
├── services/              # Los 4 agentes core
│   ├── orchestrator/
│   ├── cronista/
│   ├── guardia/
│   └── velocista/
├── shared/                # Contratos, políticas y archivos compartidos
├── .cursor/rules/         # Reglas para Cursor + Composer 2
└── README.md
```
