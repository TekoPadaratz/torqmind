// TorqMind password policy (frontend mirror).
// Mantém sincronia com apps/api/app/password_policy.py — regra única de senha.

export const PASSWORD_MIN_LENGTH = 8;
export const PASSWORD_MAX_LENGTH = 128;

// Cada regra: { key, label, test }. A ordem é a mesma do backend.
export const PASSWORD_RULES = [
  { key: "length", label: `Pelo menos ${PASSWORD_MIN_LENGTH} caracteres`, test: (p) => p.length >= PASSWORD_MIN_LENGTH },
  { key: "lowercase", label: "Uma letra minúscula (a-z)", test: (p) => /[a-z]/.test(p) },
  { key: "uppercase", label: "Uma letra maiúscula (A-Z)", test: (p) => /[A-Z]/.test(p) },
  { key: "digit", label: "Um número (0-9)", test: (p) => /\d/.test(p) },
  { key: "special", label: "Um caractere especial (ex.: ! @ # $ % & *)", test: (p) => /[^A-Za-z0-9]/.test(p) },
];

// Retorna a lista de regras com o estado de atendimento da senha informada.
export function evaluatePassword(password) {
  const pw = String(password ?? "");
  return PASSWORD_RULES.map((rule) => ({ key: rule.key, label: rule.label, ok: rule.test(pw) }));
}

// Retorna a lista de descrições das regras NÃO atendidas (vazio = válida).
export function validatePassword(password) {
  const pw = String(password ?? "");
  const errors = PASSWORD_RULES.filter((rule) => !rule.test(pw)).map((rule) => rule.label);
  if (pw.length > PASSWORD_MAX_LENGTH) {
    errors.push(`No máximo ${PASSWORD_MAX_LENGTH} caracteres`);
  }
  return errors;
}

export function isValidPassword(password) {
  return validatePassword(password).length === 0;
}
