#!/usr/bin/env python3
# save_client_info.py

import logging, sys, pathlib
LOG_PATH = pathlib.Path(__file__).with_name('save_client_info.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)   # ← остаётся вывод в stdout
    ]
)
log = logging.getLogger(__name__)
# END LOGGING
"""
Скрипт читает JSON клиента из stdin,  ➜
  • сохраняет его в clients_info.txt
  • формирует заказ и отправляет в API 1С
"""

import sys, json, uuid, datetime as dt
import requests
from pathlib import Path

# ────────────────────────────────────────────────────────────────
# 1. Настройки окружения
BASE_DIR = Path(__file__).resolve().parent

with open(BASE_DIR / "data/auth.json", encoding="utf-8") as f:
    auth = json.load(f)                 # TOKEN_1C, LOGIN_1C, PASSWORD_1C …

with open(BASE_DIR / "data/config.json", encoding="utf-8") as f:
    cfg = json.load(f)                  # proxy_url, order_path, ws_paths …

with open(BASE_DIR / "data/order_template.json", encoding="utf-8") as f:
    ORDER_TEMPLATE = json.load(f)       # шаблон заказа (dict)

MSK_TZ = dt.timezone(dt.timedelta(hours=3))

# ────────────────────────────────────────────────────────────────
def russian_address(addr: dict) -> str:
    """Собрать строковый адрес из частей"""
    city   = addr.get("city",   "")
    street = addr.get("street", "")
    house  = addr.get("house_number", "")
    return ", ".join(part for part in [city, street, house] if part)

def save_to_txt(client: dict) -> None:
    """Сохраняем информацию клиента в текстовый файл (как раньше)"""
    a = client.get("address", {})
    with open(BASE_DIR / "clients_info.txt", "a", encoding="utf-8") as file:
        file.write(f"Имя: {client.get('name','')}\n")
        file.write(f"Цель обращения: {client.get('direction','')}\n")
        file.write(f"Подробности неисправности: {client.get('circumstances','')}\n")
        file.write(f"Бренд и модель техники: {client.get('brand','')}\n")
        file.write(f"Телефон: {client.get('phone','')}\n")
        file.write(f"Второй телефон (callerID): {client.get('phone2','')}\n")
        file.write(f"Адрес: {russian_address(a)}\n")
        file.write(f"Квартира: {a.get('apartment','')}\n")
        file.write(f"Подъезд: {a.get('entrance','')}\n")
        file.write(f"Этаж: {a.get('floor','')}\n")
        file.write(f"Код домофона: {a.get('intercom','')}\n")
        file.write(f"Дата визита: {client.get('date','')}\n")
        file.write(f"Комментарий: {client.get('comment','')}\n")
        file.write("\n")

def build_order(client: dict) -> dict:
    """Формируем тело заказа на основе шаблона"""
    order = json.loads(json.dumps(ORDER_TEMPLATE))  # глубокая копия
    now   = dt.datetime.now(MSK_TZ)
    uid   = now.strftime("%Y%m%d%H%M%S") + uuid.uuid4().hex[:6]

    # ─ базовые поля ─
    order["order"]["uslugi_id"]                 = uid
    order["order"]["services"][0]["service_id"] = client.get("direction", "")
    # дата приходит как "2025-06-18" → делаем ISO-8601 с Z
    visit_raw  = client.get("date", now.date().isoformat())
    order["order"]["desired_dt"] = f"{visit_raw}T00:00Z"

    order["order"]["client"]["display_name"] = client.get("name", "")
    order["order"]["client"]["phone"]        = client.get("phone", "")

    # ─ адрес ─
    addr = client.get("address", {})
    order["order"]["address"]["name"]      = russian_address(addr)
    order["order"]["address"]["apartment"] = addr.get("apartment", "")
    order["order"]["address"]["entrance"]  = addr.get("entrance", "")
    order["order"]["address"]["floor"]     = addr.get("floor", "")
    order["order"]["address"]["intercom"]  = addr.get("intercom", "")

    # координаты, если пришли
    lat = addr.get("latitude")
    lon = addr.get("longitude")
    if lat is not None and lon is not None:
        order["order"]["address"]["geopoint"]["latitude"]  = lat
        order["order"]["address"]["geopoint"]["longitude"] = lon

    # ─ комментарий ─
    order["order"]["comment"] = " - ".join(
        part for part in [
            client.get("circumstances", ""),
            client.get("brand", ""),
            client.get("comment", "")
        ] if part
    )

    return uid, order

def send_order(uid: str, order: dict) -> None:
    """Отправляем заказ и пытаемся вернуть номер заявки"""
    order_data = {"clientPath": cfg["order_path"]}
    order_url  = cfg["proxy_url"].rstrip("/") + "/hs"
    token      = auth["TOKEN_1C"]

    payload = {"config": order_data, "params": order, "token": token}

    # сохраняем в файл для отладки
    with open(BASE_DIR / f"order_sent_{uid}.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # ─ POST в 1С ─
    try:
        resp = requests.post(order_url, json=payload, timeout=30, verify=False)

        # ↓↓ вставь до resp.raise_for_status() ↓↓
        if resp.status_code >= 400:
            print("== Тело ответа 1С ==")
            try:
                print(resp.json())          # если это JSON
            except Exception:
                print(resp.text)            # иначе как текст
            print("====================")

        resp.raise_for_status()

        print("Заказ передан в 1С (HTTP", resp.status_code, ")")
    except requests.RequestException as err:
        print("❌ Ошибка при создании заявки:", err,
              "| Тело ответа:", getattr(err, "response", None) and err.response.text)
        return

    # ─ Запрашиваем номер заявки ─
    ws_url = cfg["proxy_url"].rstrip("/") + "/ws"
    ws_payload = {
        "config": {
            "clientPath": cfg["ws_paths"],
            "login":  auth["LOGIN_1C"],
            "password": auth["PASSWORD_1C"],
        },
        "params": {
            "Идентификатор": "new_bid_number",
            "НомерПартнера": uid
        },
        "token": token
    }

    try:
        ws_resp = requests.post(ws_url, json=ws_payload, timeout=30, verify=False)
        ws_resp.raise_for_status()
        result = ws_resp.json().get("result", {})
        req_number = next(
            (item["id"] for val in result.values() if val for item in val),
            None
        )
        if req_number:
            print("✅ Номер новой заявки:", req_number)
        else:
            print("⚠ Заявка создана, но номер вернуть не удалось (проверьте логи 1С).")
    except requests.RequestException as err:
        print("⚠ Заявка создана, но не удалось узнать номер:", err)

# ────────────────────────────────────────────────────────────────
def main():
    # читаем данные JSON из stdin (как и раньше)
    raw_json = sys.stdin.read().strip()
    if not raw_json:
        print("Нет входных данных.")
        return

    try:
        client = json.loads(raw_json)
    except json.JSONDecodeError as e:
        print("Неверный JSON:", e)
        return

    # 1) записываем в текстовый файл
    save_to_txt(client)

    # 2) формируем заказ и отправляем в 1С
    uid, order = build_order(client)
    send_order(uid, order)

# ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
