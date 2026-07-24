/**
 * Ordenação canônica de grids BI.
 * Contrato: `.cursor/rules/08-grids-colunas-ordenacao.mdc`
 * Implementação: `./grid-sort.mjs`
 */

export type GridSortKeys = {
  filial?: string | number | null;
  data?: string | number | Date | null;
  nome?: string | null;
};

export {
  compareGridRows,
  sortGridRows,
} from "./grid-sort.mjs";
