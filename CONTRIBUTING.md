# Contribuir (CONTRIBUTING)

¡Gracias por tu interés en contribuir!

## Formas de contribuir
- Reportar bugs (Issues)
- Proponer mejoras (Issues / Discussions)
- Enviar Pull Requests (PRs)
- Mejorar documentación

## Antes de abrir un Issue
- Asegúrate de estar usando la última versión (`main` o último release/tag).
- Revisa Issues existentes para evitar duplicados.
- Incluye:
  - Qué esperabas que pasara vs. qué pasó
  - Pasos para reproducir
  - Logs (sin secretos)
  - Tu entorno (SO, Docker/Compose, etc.)

## Desarrollo local (Docker)
1) Copia el `.env`:
```bash
cp .env.example .env
```
2) Rellena valores reales en `.env` (no lo subas al repo).
3) Arranca:
```bash
docker-compose up -d --build
docker-compose logs -f web
```

## Estilo y calidad
- Mantén cambios pequeños y con propósito.
- Evita introducir secretos:
  - No comitees `.env`, `data/`, `tmp/` ni bases de datos.
- Intenta mantener el código legible (nombres claros, funciones pequeñas).

## Pull Requests
1) Crea una rama:
```bash
git checkout -b feature/mi-mejora
```
2) Commits claros:
- Ejemplo: `Fix scheduler TZ handling`
- Ejemplo: `Add safer schedule scope confirmation`

3) Abre PR contra `main` e incluye:
- Qué cambia y por qué
- Capturas si afecta UI
- Consideraciones de compatibilidad/migración (si aplica)

## Seguridad
Si encuentras una vulnerabilidad, no abras un Issue público. Ver `SECURITY.md`.

Gracias por contribuir 🙌
