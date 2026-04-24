FROM debian:12-slim

ENV POWERSHELL_TELEMETRY_OPTOUT=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends wget ca-certificates \
 && wget -q https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb \
 && dpkg -i packages-microsoft-prod.deb \
 && rm packages-microsoft-prod.deb \
 && apt-get update \
 && apt-get install -y --no-install-recommends powershell \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY backend /app/backend

EXPOSE 8787
ENTRYPOINT ["pwsh", "-File", "/app/backend/server.ps1"]
