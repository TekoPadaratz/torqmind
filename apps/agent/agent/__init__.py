"""TorqMind Extractor Agent 2.0 Turbo."""

# Semver do binário Windows. OBRIGATÓRIO incrementar a cada alteração em apps/agent/
# (dataset, query, watermark, sink, runtime) antes de gerar/publicar o .exe.
# Prova: `torqmind-agent.exe --version` deve bater com esta string.
__version__ = "2.0.5"

__all__ = ["__version__"]
