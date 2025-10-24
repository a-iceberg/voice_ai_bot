"""
Скрипт читает JSON клиента из stdin,  =>
  • сохраняет его в clients_info.txt
  • формирует заказ и отправляет в API 1С
"""

import logging, sys, pathlib
LOG_PATH = pathlib.Path(__file__).with_name('save_client_info.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) 
    ]
)
log = logging.getLogger(__name__)

import sys, json, uuid, datetime as dt
import requests
from pathlib import Path
import re
from datetime import datetime

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
    """Сохраняем информацию клиента в текстовый файл"""
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
    order = json.loads(json.dumps(ORDER_TEMPLATE))
    now   = dt.datetime.now(MSK_TZ)
    uid_   = now.strftime("%Y%m%d%H%M%S") + uuid.uuid4().hex[:6]

    # базовые поля
    phone_incoming = client.get("phone2") or client.get("phoneIncoming") or client.get("phone")
    phone_digits = ''.join(filter(str.isdigit, phone_incoming))
    uid = now.strftime("%Y%m%d%H%M%S") + phone_digits
    order["order"]["uslugi_id"] = uid
    order["order"]["services"][0]["service_id"] = client.get("direction", "")
    # дата приходит как "2025-06-18" => делаем ISO-8601 с Z
    visit_raw  = client.get("date", now.date().isoformat())

    now = datetime.now()
    # дата из клиента (что сказал бот)
    visit_raw_raw = (client.get("date") or "").strip()
    visit_raw = visit_raw_raw if visit_raw_raw else None
    # дата из шаблона (гарантированно есть)
    template_dt = ORDER_TEMPLATE["order"]["desired_dt"]
    # формируем значение
    if visit_raw:
        candidate_dt = f"{visit_raw}T00:00Z"
    else:
        candidate_dt = template_dt
    # проверка валидности
    iso_midnight = re.compile(r"^\d{4}-\d{2}-\d{2}T00:00Z$")
    if not iso_midnight.match(candidate_dt):
        # если формат битый — fallback на шаблон
        candidate_dt = template_dt
    order["order"]["desired_dt"] = candidate_dt

    #модель техники: бренд и модель одной строкой
    order["order"]["modelTechnique"] = client.get("brand", "")

    order["order"]["client"]["display_name"] = client.get("name", "")
    order["order"]["client"]["phone"]        = client.get("phone", "")
    order["order"]["client"]["phoneIncoming"] = client.get("phone2", "")

    # адрес 
    addr = client.get("address", {})
    order["order"]["address"]["name"]      = russian_address(addr)
    order["order"]["address"]["apartment"] = addr.get("apartment", "")
    order["order"]["address"]["entrance"]  = addr.get("entrance", "")
    order["order"]["address"]["floor"]     = addr.get("floor", "")
    order["order"]["address"]["intercom"]  = addr.get("intercom", "")

    order["order"]["multipleRequest"]   = bool(client.get("multipleRequest", order["order"].get("multipleRequest", False)))

    # name_components: проставляем город
    city = (addr or {}).get("city")
    if city:
        # гарантируем структуру name_components вида [{'kind': 'locality', 'name': <город>}]
        order["order"]["address"].setdefault("name_components", [])
        # ищем существующий элемент с kind == 'locality'
        loc_idx = next((i for i, it in enumerate(order["order"]["address"]["name_components"])
                        if isinstance(it, dict) and it.get("kind") == "locality"), None)
        if loc_idx is None:
            order["order"]["address"]["name_components"].append({"kind": "locality", "name": city})
        else:
            order["order"]["address"]["name_components"][loc_idx]["name"] = city

    # координаты, если пришли
    lat = addr.get("latitude")
    lon = addr.get("longitude")
    if lat is not None and lon is not None:
        order["order"]["address"].setdefault("geopoint", {})
        order["order"]["address"]["geopoint"]["latitude"]  = lat
        order["order"]["address"]["geopoint"]["longitude"] = lon
    

    # комментарий 
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

    # POST в 1С
    try:
        resp = requests.post(order_url, json=payload, timeout=30, verify=False)

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

    # Запрашиваем номер заявки
    ws_url = cfg["proxy_url"].rstrip("/") + "/ws"
    def _get_locality(order: dict) -> str:
        comps = (order["order"]["address"] or {}).get("name_components", [])
        for it in comps:
            if isinstance(it, dict) and it.get("kind") == "locality" and it.get("name"):
                return it["name"]
        return (order["order"]["address"].get("name") or "").split(",")[0].strip()

    city = _get_locality(order)
    if "Санкт" in city or "Петербург" in city or "СПб" in city:
        cp = "spb"
    elif "Москва" in city or "Моск" in city:
        cp = "msk"
    else:
        cp = "reg"
    
    ws_path = (cfg.get("ws_paths") or {}).get(cp)
    if not ws_path:
        raise RuntimeError(f"Не найден ws_path для cp='{cp}'")

    ws_client_path = {cp: ws_path}

    phone_incoming = (order["order"].get("client", {}) or {}).get("phoneIncoming") or ""
    phone_digits = ''.join(filter(str.isdigit, phone_incoming))
    partner_id = phone_digits or "0"

    ws_payload = {
        "config": {
            "clientPath": ws_client_path,
            "login":  auth["LOGIN_1C"],
            "password": auth["PASSWORD_1C"],
        },
        "params": {
            "Идентификатор": "new_bid_number",
            "ИДЧата": str(partner_id)
        },
        "token": token
    }
    print("[WS] cp =", cp, "| clientPath =", ws_client_path, "| ИДЧата =", partner_id)

    try:
        ws_resp = requests.post(ws_url, json=ws_payload, timeout=30, verify=False)
        if ws_resp.status_code >= 400:
            try: print("== Тело ответа /ws ==", ws_resp.json(), sep="\n")
            except Exception: print("== Тело ответа /ws ==", ws_resp.text, sep="\n")
        ws_resp.raise_for_status()
        result = ws_resp.json().get("result", {})
        req_number = next(
            (item["id"] for val in result.values() if val for item in val),
            None
        )
        if req_number:
            print("✅ Номер новой заявки:", req_number)
        else:
            print("== /ws result debug ==", json.dumps(result, ensure_ascii=False, indent=2))
            print("⚠ Заявка создана, но номер вернуть не удалось (проверьте логи 1С).")
    except requests.RequestException as err:
        print("⚠ Заявка создана, но не удалось узнать номер:", err)

# ────────────────────────────────────────────────────────────────
def main():
    # читаем данные JSON из stdin
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
