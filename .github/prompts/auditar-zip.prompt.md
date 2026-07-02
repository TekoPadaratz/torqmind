---
name: auditar-zip
description: "Auditar ZIP de branch TorqMind contra versão anterior e encontrar regressões."
agent: "TorqMind Código"
---

# Auditar ZIP TorqMind

ZIP: `${input:zip}`

## Procedimento

1. Extrair em pasta temporária.
2. Listar arquivos alterados.
3. Comparar com ZIP/base anterior se houver.
4. Rodar checagens estáticas: compileall, pycache, UTF-8 em migrations, segredos, campos sensíveis, TODO/gambiarra.
5. Validar arquitetura: API protege rotas, frontend não faz regra pesada, migrations seguras, marts usadas são populadas, roles/permissões respeitadas.

```bash
python -m compileall apps/api apps/cdc_consumer
find . -type d -name "__pycache__" -o -name "*.pyc"
python - <<'PY'
from pathlib import Path
bad=[]
for p in Path('sql/migrations').glob('*.sql'):
    try: p.read_text(encoding='utf-8')
    except Exception as e: bad.append((str(p), str(e)))
print(bad or 'UTF8_OK')
PY
```

Responder PASS/FAIL/CONDITIONAL, arquivos alterados, bugs objetivos, riscos e prompt cirúrgico.
