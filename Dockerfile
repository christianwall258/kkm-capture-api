FROM mcr.microsoft.com/powershell:7.4-debian-bookworm-slim

WORKDIR /app
COPY backend /app/backend

EXPOSE 8787
ENTRYPOINT ["pwsh", "-File", "/app/backend/server.ps1"]