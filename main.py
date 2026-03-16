"""
Finansist → Google Sheets Data Sync Engine
Точка входа. Загружает конфигурацию, настраивает логирование и запускает синхронизацию.

Использование:
    python main.py                   # Инкрементальная синхронизация
    python main.py --force           # Полная перезапись (игнорирует SQLite)
    python main.py --plan-only       # Только ПЛАН (PaymentRequests)
    python main.py --fact-only       # Только ФАКТ (Payments)
    python main.py --discover EP     # Отладка: сырой ответ эндпоинта (напр. Payments)
    python main.py --date-from YYYY-MM-DD --date-to YYYY-MM-DD
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

from src.database import SyncDatabase
from src.finansist_api import FinansistClient
from src.gsheets_client import GSheetsClient
from src.processor import SyncProcessor


def setup_logging(level: str = "INFO") -> None:
    """Настраивает форматированное логирование в stdout."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    # Принудительно UTF-8 для Windows (cp1251 не поддерживает кириллицу в логах)
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    logging.basicConfig(
        level=log_level,
        format=fmt,
        datefmt=datefmt,
        stream=sys.stdout,
        force=True,
    )

    # Гасим шумный лог httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def load_config() -> dict:
    """Загружает конфигурацию из переменных окружения (.env или реальных env)."""
    load_dotenv()

    required = [
        "FINANSIST_BASE_URL",
        "FINANSIST_API_KEY",
        "GOOGLE_SPREADSHEET_ID",
        "GOOGLE_CREDENTIALS_PATH",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"ОШИБКА: Не заданы обязательные переменные окружения: {', '.join(missing)}", file=sys.stderr)
        print("Скопируйте .env.example → .env и заполните значения.", file=sys.stderr)
        sys.exit(1)

    return {
        "finansist_base_url": os.environ["FINANSIST_BASE_URL"],
        "finansist_api_key": os.environ["FINANSIST_API_KEY"],
        "spreadsheet_id": os.environ["GOOGLE_SPREADSHEET_ID"],
        "credentials_path": os.environ["GOOGLE_CREDENTIALS_PATH"],
        "db_path": os.getenv("DB_PATH", "data/sync_state.db"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "page_size": int(os.getenv("PAGE_SIZE", "100")),
        "http_timeout": float(os.getenv("HTTP_TIMEOUT", "30")),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Синхронизация Finansist → Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Полная перезапись: игнорировать SQLite и записать все операции заново",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Синхронизировать только ПЛАН (PaymentRequests)",
    )
    parser.add_argument(
        "--fact-only",
        action="store_true",
        help="Синхронизировать только ФАКТ (Payments)",
    )
    parser.add_argument(
        "--date-from",
        metavar="YYYY-MM-DD",
        help="Дата начала периода выборки (включительно)",
    )
    parser.add_argument(
        "--date-to",
        metavar="YYYY-MM-DD",
        help="Дата конца периода выборки (включительно)",
    )
    parser.add_argument(
        "--discover",
        metavar="ENDPOINT",
        help="Отладочный режим: вывести сырой ответ API для указанного эндпоинта и выйти",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Уровень логирования (переопределяет LOG_LEVEL из .env)",
    )
    return parser.parse_args()


def run_discover(api: FinansistClient, endpoint: str) -> None:
    """Отладочный режим: выводит сырой ответ эндпоинта."""
    logger = logging.getLogger("discover")
    logger.info("Запрос к эндпоинту: %s", endpoint)
    result = api.discover(endpoint)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def print_stats(stats: dict) -> None:
    """Выводит итоговую статистику в консоль."""
    sep = "─" * 55
    print(f"\n{sep}")
    print("  ИТОГИ СИНХРОНИЗАЦИИ")
    print(sep)

    for section_key, section_label in [("plan", "ПЛАН"), ("fact", "ФАКТ")]:
        s = stats.get(section_key, {})
        if not s:
            continue
        print(f"\n  {section_label}:")
        print(f"    Всего обработано : {s.get('total', 0)}")
        print(f"    Новых записей    : {s.get('new', 0)}")
        print(f"    Обновлено статус : {s.get('updated', 0)}")
        print(f"    Уже в таблице    : {s.get('skipped', 0)}")

    db_stats = stats.get("db", {})
    if db_stats:
        print(f"\n  База состояния (SQLite):")
        print(f"    ПЛАН             : {db_stats.get('plan', 0)} записей")
        print(f"    ФАКТ             : {db_stats.get('fact', 0)} записей")

    errors = stats.get("errors", [])
    if errors:
        print(f"\n  Ошибки ({len(errors)}):")
        for err in errors[:10]:
            print(f"    - {err}")
        if len(errors) > 10:
            print(f"    ... и ещё {len(errors) - 10}")

    print(f"\n{sep}\n")


def main() -> int:
    args = parse_args()
    cfg = load_config()

    log_level = args.log_level or cfg["log_level"]
    setup_logging(log_level)
    logger = logging.getLogger("main")

    logger.info("=== Finansist -> Google Sheets Sync ===")
    logger.info("Режим: %s%s%s",
                "FORCE " if args.force else "",
                "PLAN-ONLY " if args.plan_only else "",
                "FACT-ONLY" if args.fact_only else ("" if args.plan_only else "PLAN+FACT"),
                )

    # Создаём директорию для БД если нужно
    db_path = Path(cfg["db_path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with FinansistClient(
        base_url=cfg["finansist_base_url"],
        api_key=cfg["finansist_api_key"],
        page_size=cfg["page_size"],
        timeout=cfg["http_timeout"],
    ) as api:

        # Отладочный режим --discover
        if args.discover:
            run_discover(api, args.discover)
            return 0

        sheets = GSheetsClient(
            spreadsheet_id=cfg["spreadsheet_id"],
            credentials_path=cfg["credentials_path"],
        )
        db = SyncDatabase(str(db_path))

        processor = SyncProcessor(
            api=api,
            sheets=sheets,
            db=db,
            sync_plan=not args.fact_only,
            sync_fact=not args.plan_only,
            force=args.force,
            date_from=args.date_from,
            date_to=args.date_to,
        )

        try:
            stats = processor.run()
        except KeyboardInterrupt:
            logger.warning("Прервано пользователем (Ctrl+C)")
            return 130
        except Exception:
            logger.exception("Критическая ошибка при синхронизации")
            return 1
        finally:
            db.close()

    print_stats(stats)
    logger.info("Синхронизация завершена.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
