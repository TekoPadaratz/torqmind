#!/usr/bin/env python3
"""Relatório PDF: CFOPs de saída do dia — Considerado vs Não considerado.

Uso (API container com ClickHouse):
  docker exec -i torqmind-api python - <<'PY'
  # ou no repo:
  PYTHONPATH=apps/api python3 tools/report_sales_cfop_day_pdf.py \\
    --id-empresa 1 --id-filial 14458 --dia 2026-08-17 --out /tmp/cfop-vr01.pdf

Considerado = CFOP > 5000 excluindo 5927/5929/6929 (venda canônica TorqMind).
Não considerado = entrada, devolução, perda, transferência ou sem CFOP.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps" / "api"))

SALES_EXCLUDED = {5927, 5929, 6929}
RETURN_EXIT = {5202, 5411, 6202, 6411}


def _parse_day(raw: str) -> date:
    return date.fromisoformat(raw)


def _money(v: Any) -> str:
    n = float(v or 0)
    return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _classify(cfop: int) -> Tuple[str, str]:
    if cfop <= 0:
        return "nao_considerado", "sem CFOP"
    if cfop in RETURN_EXIT:
        return "nao_considerado", "devolucao (saida)"
    if cfop in SALES_EXCLUDED:
        return "nao_considerado", "perda/transferencia"
    if cfop > 5000:
        return "considerado", "venda/saida"
    if 1000 <= cfop <= 3999:
        return "nao_considerado", "entrada"
    return "nao_considerado", "outro"


def _fetch_rows(id_empresa: int, id_filial: int | None, dia: date) -> List[Dict[str, Any]]:
    from app.db_clickhouse import query_dict

    filial_sql = f"AND c.id_filial = {int(id_filial)}" if id_filial else ""
    key = int(dia.strftime("%Y%m%d"))
    return query_dict(
        f"""
        SELECT
          coalesce(i.cfop, 0) AS cfop,
          round(sum(i.total), 2) AS valor,
          uniqExact(c.id_empresa, c.id_filial, c.id_db, c.id_comprovante) AS qtd_docs,
          toUInt32(count()) AS qtd_itens
        FROM torqmind_current.stg_comprovantes_slim AS c FINAL
        INNER JOIN torqmind_current.stg_itenscomprovantes_slim AS i FINAL
          ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
         AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
        WHERE c.id_empresa = {{id_empresa:Int32}}
          AND c.data_key = {{data_key:Int32}}
          AND c.is_deleted = 0 AND i.is_deleted = 0
          AND c.commercial_eligible = 1
          {filial_sql}
        GROUP BY cfop
        ORDER BY valor DESC
        """,
        {"id_empresa": int(id_empresa), "data_key": key},
    )


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_pdf_bytes(lines: List[str]) -> bytes:
    """PDF mínimo (Helvetica) sem dependências externas."""
    y = 800
    content_lines = ["BT", "/F1 10 Tf", "14 TL", f"40 {y} Td"]
    first = True
    for line in lines:
        escaped = _pdf_escape(line[:110])
        if first:
            content_lines.append(f"({escaped}) Tj")
            first = False
        else:
            content_lines.append(f"T* ({escaped}) Tj")
    content_lines.append("ET")
    stream = "\n".join(content_lines).encode("latin-1", errors="replace")

    objects: List[bytes] = []
    objects.append(b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n")
    objects.append(b"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n")
    objects.append(
        b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>endobj\n"
    )
    objects.append(
        f"4 0 obj<< /Length {len(stream)} >>stream\n".encode("ascii")
        + stream
        + b"\nendstream\nendobj\n"
    )
    objects.append(b"5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n")

    out = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(len(out))
        out.extend(obj)
    xref_pos = len(out)
    out.extend(f"xref\n0 {len(offsets)}\n".encode("ascii"))
    out.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        out.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    out.extend(
        f"trailer<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode(
            "ascii"
        )
    )
    return bytes(out)


def build_pdf(
    *,
    id_empresa: int,
    id_filial: int | None,
    dia: date,
    rows: List[Dict[str, Any]],
    out_path: Path,
) -> Path:
    filial_lbl = f"filial {id_filial}" if id_filial else "todas as filiais"
    lines: List[str] = [
        "TorqMind - CFOPs do dia (Considerado x Nao considerado)",
        f"Empresa {id_empresa} | {filial_lbl} | dia {dia.strftime('%d/%m/%Y')}",
        f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        "",
        "Considerado = CFOP > 5000 sem 5927/5929/6929 (venda canonica).",
        "Nao considerado = entrada, devolucao, perda, transferencia ou sem CFOP.",
        "",
        f"{'CFOP':<8}{'Classe':<22}{'Status':<18}{'Docs':>6}{'Itens':>8}{'Valor':>16}",
        "-" * 78,
    ]
    buckets: Dict[str, float] = defaultdict(float)
    for r in rows:
        cfop = int(r.get("cfop") or 0)
        status, classe = _classify(cfop)
        valor = float(r.get("valor") or 0)
        buckets[status] += valor
        status_lbl = "Considerado" if status == "considerado" else "Nao considerado"
        lines.append(
            f"{str(cfop or '-'):<8}{classe:<22}{status_lbl:<18}"
            f"{int(r.get('qtd_docs') or 0):>6}{int(r.get('qtd_itens') or 0):>8}"
            f"{_money(valor):>16}"
        )
    lines.append("-" * 78)
    lines.append(f"TOTAL CONSIDERADO:     {_money(buckets['considerado'])}")
    lines.append(f"TOTAL NAO CONSIDERADO: {_money(buckets['nao_considerado'])}")
    out_path.write_bytes(_build_pdf_bytes(lines))
    # Também grava .txt ao lado para auditoria rápida
    out_path.with_suffix(".txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--id-empresa", type=int, default=1)
    ap.add_argument("--id-filial", type=int, default=None)
    ap.add_argument("--dia", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", default=None, help="Caminho do PDF")
    args = ap.parse_args()
    dia = _parse_day(args.dia)
    out = Path(
        args.out
        or f"/tmp/torqmind-cfop-{args.id_empresa}-{args.id_filial or 'all'}-{dia.isoformat()}.pdf"
    )
    rows = _fetch_rows(args.id_empresa, args.id_filial, dia)
    build_pdf(
        id_empresa=args.id_empresa,
        id_filial=args.id_filial,
        dia=dia,
        rows=rows,
        out_path=out,
    )
    print(f"OK pdf={out} txt={out.with_suffix('.txt')} cfops={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
