import axios from "axios";

export function isRequestCanceled(error) {
  return Boolean(
    axios.isCancel(error) ||
      error?.code === "ERR_CANCELED" ||
      error?.name === "CanceledError" ||
      error?.name === "AbortError",
  );
}
