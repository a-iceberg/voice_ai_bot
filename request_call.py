import sys
import json
import logging
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_PATH = LOG_DIR / "request_call.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

with open(BASE_DIR / "data/auth.json", encoding="utf-8") as f:
    auth = json.load(f)

with open(BASE_DIR / "data/config.json", encoding="utf-8") as f:
    cfg = json.load(f)

with open(BASE_DIR / "data/call_template.json", encoding="utf-8") as f:
    CALL_TEMPLATE = json.load(f)

DIRECTION_ALIASES = {
    "Вывоз мусора": "Вывоз мусора партнеры",
    "Вскрытие и установка замков": "Вскрытие замков",
}


def normalize_direction(value: str) -> str:
    v = (value or "").strip()
    return DIRECTION_ALIASES.get(v, v)


def build_call_params(client: dict) -> dict:
    call_params = json.loads(json.dumps(CALL_TEMPLATE))
    rl = call_params["requestList"]

    message = (client.get("message") or "").strip()
    if not message:
        circumstances = (client.get("circumstances") or "").strip()
        comment = (client.get("comment") or "").strip()
        message = "; ".join(p for p in [circumstances, comment] if p)
    rl["customer_message"] = message

    rl["customer_name"] = client.get("name", "") or ""
    rl["customer_phone"] = client.get("phone", "") or ""
    rl["direction"] = normalize_direction(client.get("direction", ""))
    rl["model"] = client.get("brand", "") or ""
    rl["city"] = client.get("city", "") or ""
    rl["utm_source"] = client.get("did", "") or ""

    return call_params


def request_call(client: dict) -> None:
    call_params = build_call_params(client)

    call_url = cfg["proxy_url"].rstrip("/") + "/ws"
    call_data = {
        "clientPath": cfg["call_path"],
        "login": auth["LOGIN_CALL"],
        "password": auth["PASSWORD_CALL"],
    }
    payload = {"config": call_data, "params": call_params, "token": auth["TOKEN_1C"]}

    log.info("Запрос обратного звонка: %s", json.dumps(call_params["requestList"], ensure_ascii=False))

    try:
        resp = requests.post(call_url, json=payload, timeout=90)
        if resp.status_code >= 400:
            try:
                log.error("== Тело ответа /ws ==\n%s", resp.json())
            except Exception:
                log.error("== Тело ответа /ws ==\n%s", resp.text)
        resp.raise_for_status()
        try:
            result = resp.json()
        except Exception:
            log.info("Заявка на звонок отправлена (HTTP %s), тело не JSON: %s", resp.status_code, resp.text)
            return

        log.info("status: %s", result)
        phone = call_params["requestList"].get("customer_phone", "")

        if result.get("status") == "ok":
            log.info("✅ Заявка на обратный звонок создана для телефона %s", phone)
        elif result.get("detail"):
            if "дубль" in str(result["detail"]):
                log.info("Дубль заявки на обратный звонок для телефона %s", phone)
            else:
                log.error("Ошибка при запросе звонка: %s", result)
        else:
            log.error("Ошибка при запросе звонка: %s", result)
    except requests.RequestException as err:
        log.error(
            "❌ Ошибка при создании заявки на обратный звонок: %s | Тело ответа: %s",
            err,
            getattr(err, "response", None) and err.response.text,
        )


def main():
    raw_json = sys.stdin.read().strip()
    if not raw_json:
        log.error("Нет входных данных.")
        return
    try:
        client = json.loads(raw_json)
    except json.JSONDecodeError as e:
        log.error("Неверный JSON: %s", e)
        return

    if not (client.get("phone") or "").strip():
        log.error("Не задан телефон клиента — заявка на обратный звонок не создаётся.")
        return

    log.info("-" * 60)
    request_call(client)


if __name__ == "__main__":
    main()
