export const USERNAME_PATTERN = /^[a-z0-9._-]{3,32}$/;
export const USERNAME_ERROR_MESSAGE =
  "Nome de usuário deve ter entre 3 e 32 caracteres e usar apenas letras minúsculas, números, ponto, underscore ou hífen.";

export function normalizeUsernameInput(value) {
  return String(value ?? "").trim().toLowerCase();
}

export function validateUsernameInput(value) {
  const normalized = normalizeUsernameInput(value);
  if (!USERNAME_PATTERN.test(normalized)) {
    return {
      ok: false,
      normalized,
      message: USERNAME_ERROR_MESSAGE,
    };
  }
  return {
    ok: true,
    normalized,
    message: null,
  };
}
