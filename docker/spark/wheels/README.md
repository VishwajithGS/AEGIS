# Optional local package cache for the aegis-spark image

Leave this folder empty for a normal build (pip downloads everything from PyPI).

On a slow or unreliable connection, download the PySpark source archive here
first with a resumable tool, then build:

```powershell
curl.exe -L -C - --retry 10 -o docker\spark\wheels\pyspark-3.5.9.tar.gz `
  https://files.pythonhosted.org/packages/95/ce/81e53e729790556e3983e95de1a7d5df91a34adfcd34b5a5ab0e0c6e9b33/pyspark-3.5.9.tar.gz
docker compose build tools
```

The build verifies the SHA-256 (`ea27adc3…243d93`, from PyPI) before
installing it. Archives in this folder are git-ignored.
