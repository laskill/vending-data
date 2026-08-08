#!/usr/bin/env python3
"""
Сборщик транзакций Vendista -> CSV для дашборда "Пульт учёта".

Использование:
    export VENDISTA_TOKEN="63653f5300254263a2af32b9"
    python3 vendista_collector.py --from 2026-07-01 --to 2026-07-31 --out july.csv

    # Инкрементально (с последнего запуска, автоматически):
    python3 vendista_collector.py --out incremental.csv

Логика инкрементального режима:
    - Дата начала берётся из файла state.json (поле last_fetch_to)
    - Если файла нет — по умолчанию берётся последние 24 часа
    - После успешной выгрузки state.json обновляется

Токен НЕ хранится в коде — берётся из переменной окружения VENDISTA_TOKEN,
чтобы его не было видно в файле скрипта.
"""

import os
import sys
import json
import csv
import time
import argparse
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import urllib.error

API_BASE = "https://api.vendista.ru:99"
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")

# Коды статусов Vendista -> текст, который понимает дашборд
STATUS_MAP = {
    1: "Успешная транзакция снятия денег",
    2: "Успешная транзакция снятия денег, по которой впоследствии произошел возврат",
    3: "Неуспешная транзакция снятия денег",
    4: "Успешный возврат денег",
    5: "Неуспешный возврат денег",
}


def get_token():
    token = os.environ.get("VENDISTA_TOKEN")
    if not token:
        sys.exit("Ошибка: переменная окружения VENDISTA_TOKEN не задана.\n"
                 "Запустите: export VENDISTA_TOKEN=\"ваш_токен\"")
    return token


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def fetch_page(token, date_from, date_to, page_number, max_retries=4):
    params = {
        "token": token,
        "DateFrom": date_from,
        "DateTo": date_to,
        "OrderDesc": "false",
        "PageNumber": page_number,     # PascalCase — как DateFrom/DateTo/TermId, не page_number
        "ItemsPerPage": 50,            # сервер всё равно режет по 50, но параметр пусть будет явным
    }
    url = API_BASE + "/transactions?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})

    wait = 20
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "ignore")
            if e.code == 429 and attempt < max_retries:
                print(f"429 Too Many Requests — жду {wait} сек и пробую снова "
                      f"(попытка {attempt}/{max_retries})...", file=sys.stderr)
                time.sleep(wait)
                wait *= 2
                continue
            if e.code == 429:
                # Не роняем весь workflow из-за временного лимита — просто пропускаем этот запуск,
                # следующий по расписанию прогон подхватит данные с той же точки (state.json не обновится).
                print(f"429 Too Many Requests — лимит не снялся после {max_retries} попыток. "
                      f"Пропускаю этот запуск, попробую в следующий раз.", file=sys.stderr)
                sys.exit(0)
            sys.exit(f"Ошибка HTTP {e.code} при запросе к Vendista API: {body}")
        except urllib.error.URLError as e:
            if attempt < max_retries:
                print(f"Сетевая ошибка ({e.reason}) — жду {wait} сек и пробую снова...", file=sys.stderr)
                time.sleep(wait)
                wait *= 2
                continue
            sys.exit(f"Не удалось подключиться к Vendista API: {e.reason}")


def fetch_all_transactions(token, date_from, date_to):
    all_items = []
    seen_ids = set()
    page = 1
    while True:
        data = fetch_page(token, date_from, date_to, page)
        if not data.get("success", True) and "items" not in data:
            sys.exit(f"API вернул ошибку: {data}")
        items = data.get("items", [])
        if not items:
            break
        new_count = 0
        for it in items:
            iid = it.get("id")
            if iid not in seen_ids:
                seen_ids.add(iid)
                all_items.append(it)
                new_count += 1
        items_per_page = data.get("items_per_page", 50)
        if new_count == 0:
            # страница вернула только уже виденные записи — параметр страницы не сработал, дальше листать бессмысленно
            print("Внимание: страница не дала новых записей — похоже, сервер игнорирует номер страницы.", file=sys.stderr)
            break
        if len(items) < items_per_page:
            break
        page += 1
        if page > 200:  # защитный предел
            print("Внимание: остановился на 200 страницах — проверь диапазон дат.", file=sys.stderr)
            break
    return all_items


def row_from_item(it):
    amount_rub = (it.get("sum") or 0) / 100.0
    status_text = STATUS_MAP.get(it.get("status"), f"Неизвестный статус ({it.get('status')})")
    return [
        str(it.get("id")),
        it.get("time", ""),
        it.get("terminal_id", ""),
        it.get("term_id", ""),
        f"{amount_rub:.2f}".replace(".", ","),
        status_text,
    ]


def write_csv_append_dedupe(items, out_path, keep_days=90):
    """Merge new items into an existing rolling CSV, de-duplicated by ID транзакции,
    and drop rows older than keep_days so the file doesn't grow forever."""
    header = ["ID транзакции", "Дата и время", "TID", "ID терминала", "Сумма, ₽", "Статус"]
    existing = {}
    if os.path.exists(out_path):
        with open(out_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f, delimiter=";")
            rows = list(reader)
        if rows:
            for r in rows[1:]:
                if len(r) >= 6:
                    existing[r[0]] = r

    for it in items:
        row = row_from_item(it)
        existing[row[0]] = row

    cutoff = datetime.now() - timedelta(days=keep_days)

    def row_date(r):
        try:
            return datetime.strptime(r[1][:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now()

    all_rows = [r for r in existing.values() if row_date(r) >= cutoff]
    all_rows.sort(key=row_date)

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(all_rows)

    return len(all_rows)


def main():
    parser = argparse.ArgumentParser(description="Сборщик транзакций Vendista")
    parser.add_argument("--from", dest="date_from", help="Дата начала YYYY-MM-DD (по умолчанию: с последнего запуска)")
    parser.add_argument("--to", dest="date_to", help="Дата конца YYYY-MM-DD (по умолчанию: сейчас)")
    parser.add_argument("--out", dest="out", required=True, help="Путь к выходному CSV (данные дописываются и дедуплицируются)")
    parser.add_argument("--keep-days", dest="keep_days", type=int, default=90, help="Сколько дней истории хранить в файле (по умолчанию 90)")
    args = parser.parse_args()

    token = get_token()
    state = load_state()

    now = datetime.now()
    date_to = args.date_to or now.strftime("%Y-%m-%d %H:%M:%S")

    if args.date_from:
        date_from = args.date_from
    elif state.get("last_fetch_to"):
        date_from = state["last_fetch_to"]
    else:
        date_from = (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    print(f"Забираю транзакции с {date_from} по {date_to}...")
    items = fetch_all_transactions(token, date_from, date_to)
    print(f"Получено записей: {len(items)}")

    if items:
        total = write_csv_append_dedupe(items, args.out, keep_days=args.keep_days)
        print(f"Обновлено: {args.out} (строк в файле после дедупликации: {total})")
    else:
        print("Новых транзакций нет — файл не изменён.")

    state["last_fetch_to"] = date_to
    state["last_run"] = now.isoformat()
    save_state(state)


if __name__ == "__main__":
    main()
