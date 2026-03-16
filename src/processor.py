"""
Ядро бизнес-логики: нормализация операций из Finansist API,
разбивка по годам и координация записи в Google Sheets + SQLite.

Источники данных:
  ПЛАН → GET /api/PaymentRequests  (заявки на платеж)
  ФАКТ → GET /api/Payments         (фактические платежи)
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

from .finansist_api import FinansistClient, PAYMENT_REQUEST_STATUSES
from .gsheets_client import GSheetsClient
from .database import SyncDatabase

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Описание колонок Google Sheets
# (field_key, заголовок)
# Поля взяты из API-документации reportfinance-api.md
# ──────────────────────────────────────────────────────────────────────────────
COLUMNS: list[tuple[str, str]] = [
    ("id",                    "ID"),
    ("external_id",           "Внешний ID"),
    ("operation_type",        "Тип (план/факт)"),
    ("date",                  "Дата платежа"),
    ("year",                  "Год"),
    ("amount",                "Сумма"),
    ("source_amount",         "Исходная сумма"),
    ("currency",              "Валюта"),
    ("stream_id",             "ID статьи"),
    ("stream_name",           "Статья"),
    ("project_id",            "ID проекта"),
    ("project_name",          "Проект"),
    ("group_id",              "ID группы"),
    ("group_name",            "Группа"),
    ("contragent_id",         "ID контрагента"),
    ("contragent_name",       "Контрагент"),
    ("account_id",            "ID счёта"),
    ("account_name",          "Счёт"),
    ("organisation_id",       "ID организации"),
    ("organisation_name",     "Организация"),
    ("status_id",             "ID статуса"),
    ("status_name",           "Статус"),
    ("payment_for",           "Оплата за"),
    ("payment_purpose",       "Назначение платежа"),
    ("comment",               "Комментарий"),
    ("is_urgent",             "Срочная"),
    ("tags",                  "Теги"),
    ("invoice_id",            "ID счёта-фактуры"),
    ("responsible_id",        "ID ответственного"),
    ("responsible_name",      "Ответственный (менеджер)"),
]

HEADER = [col[1] for col in COLUMNS]
FIELD_KEYS = [col[0] for col in COLUMNS]
STATUS_NAME_IDX = FIELD_KEYS.index("status_name")


def _s(value: Any) -> str:
    """Безопасное приведение значения к строке для Google Sheets."""
    if value is None or value == "":
        return ""
    if isinstance(value, bool):
        return "Да" if value else "Нет"
    if isinstance(value, (list, dict)):
        return str(value)
    return str(value)


def _parse_date(date_str: Any) -> datetime | None:
    """Парсит дату в различных форматах из API Финансиста."""
    if not date_str:
        return None
    clean = str(date_str).split("+")[0].strip().rstrip("Z")
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(clean[:19], fmt) if "T" in clean else datetime.strptime(clean, fmt)
        except (ValueError, TypeError):
            continue
    # Fallback: попытка нормализовать дату с неполными компонентами (2025-8-2)
    try:
        parts = clean[:10].split("-")
        if len(parts) == 3:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, TypeError):
        pass
    logger.warning("Не удалось распарсить дату: '%s'", date_str)
    return None


def normalize_payment_request(raw: dict, lookup: "Lookup") -> dict:
    """
    Нормализует заявку на платеж (ПЛАН).

    Поля API (из ApiPaymentRequestSaveDto):
        id, externalId, paymentDate, internalPaymentDate, sourcePaymentSum,
        contragentId, paymentFor, paymentPurpose, accountId, projectId,
        organisationId, factStreamId, comment, isUrgent, statusId, tags, invoiceId
    """
    date_str = raw.get("paymentDate") or raw.get("internalPaymentDate") or ""
    date_obj = _parse_date(date_str)
    stream_id = raw.get("factStreamId") or raw.get("streamId")
    project_id = raw.get("projectId")
    contragent_id = raw.get("contragentId")
    account_id = raw.get("accountId")
    organisation_id = raw.get("organisationId")
    # API возвращает поле "status", не "statusId"
    status_id = raw.get("status") or raw.get("statusId")
    tags = raw.get("tags") or []
    tags_str = ", ".join(str(t) for t in tags) if isinstance(tags, list) else _s(tags)
    # userName/userEmail прямо в записи — не нужен lookup
    responsible_id = raw.get("userId") or raw.get("managerId")
    responsible_name = raw.get("userName") or raw.get("userEmail") or lookup.user_name(responsible_id)

    return {
        "id":               _s(raw.get("id")),
        "external_id":      _s(raw.get("externalId")),
        "operation_type":   "план",
        "date":             date_obj.strftime("%Y-%m-%d") if date_obj else date_str,
        "year":             date_obj.year if date_obj else 0,
        "amount":           _s(raw.get("paymentSum") or raw.get("sourcePaymentSum") or raw.get("amount")),
        "source_amount":    _s(raw.get("sourcePaymentSum")),
        "currency":         _s(raw.get("sourceCurrencyId") or raw.get("currency") or "RUB"),
        "stream_id":        _s(stream_id),
        "stream_name":      lookup.stream_name(stream_id),
        "project_id":       _s(project_id),
        "project_name":     lookup.project_name(project_id),
        "group_id":         _s(lookup.project_group_id(project_id)),
        "group_name":       lookup.project_group_name(project_id),
        "contragent_id":    _s(contragent_id),
        "contragent_name":  lookup.contragent_name(contragent_id),
        "account_id":       _s(account_id),
        "account_name":     lookup.account_name(account_id),
        "organisation_id":  _s(organisation_id),
        "organisation_name": lookup.organisation_name(organisation_id),
        "status_id":        _s(status_id),
        "status_name":      _s(PAYMENT_REQUEST_STATUSES.get(int(status_id), status_id) if status_id else ""),
        "payment_for":      _s(raw.get("paymentFor")),
        "payment_purpose":  _s(raw.get("paymentPurpose")),
        "comment":          _s(raw.get("comment")),
        "is_urgent":        _s(raw.get("isUrgent")),
        "tags":             tags_str,
        "invoice_id":       _s(raw.get("invoiceId")),
        "responsible_id":   _s(responsible_id),
        "responsible_name": responsible_name,
    }


def normalize_payment(raw: dict, lookup: "Lookup") -> dict:
    """
    Нормализует фактический платеж (ФАКТ).

    Поля GET /api/Payments (ApiPaymentDto + типичные поля ответа):
        id, externalId, date/paymentDate, amount/sum,
        contragentId, accountId, projectId, organisationId,
        streamId/factStreamId, comment, tags
    """
    date_str = (
        raw.get("paymentDate")
        or raw.get("internalPaymentDate")
        or raw.get("date")
        or raw.get("operationDate")
        or ""
    )
    date_obj = _parse_date(date_str)
    stream_id = raw.get("factStreamId") or raw.get("streamId")
    project_id = raw.get("projectId") or None
    contragent_id = raw.get("contragentId") or None
    account_id = raw.get("accountId")
    organisation_id = raw.get("organisationId")
    # Для Payments статус = paymentStatusId
    status_id = raw.get("paymentStatusId") or raw.get("statusId")
    tags = raw.get("tags") or []
    tags_str = ", ".join(str(t) for t in tags) if isinstance(tags, list) else _s(tags)
    # Ответственный прямо в записи
    responsible_name = raw.get("responsible") or raw.get("userName") or ""

    return {
        "id":               _s(raw.get("id")),
        "external_id":      _s(raw.get("externalId")),
        "operation_type":   "факт",
        "date":             date_obj.strftime("%Y-%m-%d") if date_obj else date_str,
        "year":             date_obj.year if date_obj else 0,
        "amount":           _s(raw.get("paymentSum") or raw.get("sourcePaymentSum") or raw.get("amount")),
        "source_amount":    _s(raw.get("sourcePaymentSum")),
        "currency":         _s(raw.get("sourceCurrencyId") or raw.get("currency") or "RUB"),
        "stream_id":        _s(stream_id),
        "stream_name":      lookup.stream_name(stream_id),
        "project_id":       _s(project_id),
        "project_name":     lookup.project_name(project_id),
        "group_id":         _s(lookup.project_group_id(project_id)),
        "group_name":       lookup.project_group_name(project_id),
        "contragent_id":    _s(contragent_id),
        "contragent_name":  lookup.contragent_name(contragent_id),
        "account_id":       _s(account_id),
        "account_name":     lookup.account_name(account_id),
        "organisation_id":  _s(organisation_id),
        "organisation_name": lookup.organisation_name(organisation_id),
        "status_id":        _s(status_id),
        "status_name":      _s(status_id),
        "payment_for":      _s(raw.get("paymentFor")),
        "payment_purpose":  _s(raw.get("paymentPurpose")),
        "comment":          _s(raw.get("comment")),
        "is_urgent":        "",
        "tags":             tags_str,
        "invoice_id":       _s(raw.get("invoiceId")),
        "responsible_id":   "",
        "responsible_name": responsible_name,
    }


def operation_to_row(op: dict) -> list[str]:
    """Преобразует нормализованную операцию в строку Google Sheets."""
    return [op.get(key, "") for key in FIELD_KEYS]


# ──────────────────────────────────────────────────────────────────────────────
# Кэш справочников для обогащения данных именами
# ──────────────────────────────────────────────────────────────────────────────

class Lookup:
    """Загружает и кэширует справочные данные для обогащения операций."""

    def __init__(self):
        self._projects: dict[int, dict] = {}
        self._groups: dict[int, str] = {}
        self._contragents: dict[int, str] = {}
        self._accounts: dict[int, str] = {}
        self._streams: dict[int, str] = {}
        self._organisations: dict[int, str] = {}
        self._users: dict[int, str] = {}

    def load(self, api: FinansistClient):
        """Загружает все справочники из API одним разом."""
        logger.info("Загрузка справочников (проекты, контрагенты, статьи...)...")

        for g in api.get_project_groups():
            gid = g.get("id")
            if gid is not None:
                self._groups[int(gid)] = g.get("name", "")

        for p in api.get_projects():
            pid = p.get("id")
            if pid is not None:
                self._projects[int(pid)] = {
                    "name": p.get("projectName", ""),
                    "groupId": p.get("groupId"),
                }

        for c in api.get_contragents():
            cid = c.get("id")
            if cid is not None:
                self._contragents[int(cid)] = (
                    c.get("name") or c.get("contragentName") or ""
                )

        for a in api.get_accounts():
            aid = a.get("id")
            if aid is not None:
                self._accounts[int(aid)] = a.get("accountName", "")

        try:
            for s in api.get_fact_streams():
                sid = s.get("id")
                if sid is not None:
                    self._streams[int(sid)] = s.get("name", "")
        except Exception as e:
            logger.warning("FactStreams недоступны: %s", e)

        try:
            for s in api.get_streams():
                sid = s.get("id")
                if sid is not None:
                    self._streams.setdefault(int(sid), s.get("name", ""))
        except Exception as e:
            logger.warning("Streams недоступны: %s", e)

        try:
            for o in api.get_organisations():
                oid = o.get("id")
                if oid is not None:
                    self._organisations[int(oid)] = (
                        o.get("name") or o.get("organisationName") or ""
                    )
        except Exception as e:
            logger.warning("Organisations недоступны: %s", e)

        try:
            users_list = api.get_users()
            if users_list:
                logger.debug("Users первый элемент: %s", users_list[0])
            for u in users_list:
                uid = u.get("id")
                if uid is not None:
                    name = (
                        u.get("name")
                        or u.get("fullName")
                        or f"{u.get('firstName', '')} {u.get('lastName', '')}".strip()
                        or u.get("login", "")
                        or u.get("email", "")
                    )
                    if name:
                        self._users[int(uid)] = name
        except Exception as e:
            logger.warning("Users недоступны: %s", e)

        try:
            managers_list = api.get_managers()
            if managers_list:
                logger.debug("Managers первый элемент: %s", managers_list[0])
            for m in managers_list:
                mid = m.get("id")
                if mid is not None:
                    name = (
                        m.get("name")
                        or m.get("fullName")
                        or f"{m.get('firstName', '')} {m.get('lastName', '')}".strip()
                    )
                    if name:
                        self._users.setdefault(int(mid), name)
        except Exception as e:
            logger.warning("Managers недоступны: %s", e)

        logger.info(
            "Справочники загружены: проектов=%d, групп=%d, контрагентов=%d, счетов=%d, статей=%d, пользователей=%d",
            len(self._projects), len(self._groups),
            len(self._contragents), len(self._accounts), len(self._streams), len(self._users),
        )

    def project_name(self, pid: Any) -> str:
        if pid is None:
            return ""
        try:
            return self._projects.get(int(pid), {}).get("name", _s(pid))
        except (ValueError, TypeError):
            return _s(pid)

    def project_group_id(self, pid: Any) -> Any:
        if pid is None:
            return ""
        try:
            return self._projects.get(int(pid), {}).get("groupId", "")
        except (ValueError, TypeError):
            return ""

    def project_group_name(self, pid: Any) -> str:
        gid = self.project_group_id(pid)
        if not gid:
            return ""
        try:
            return self._groups.get(int(gid), _s(gid))
        except (ValueError, TypeError):
            return _s(gid)

    def contragent_name(self, cid: Any) -> str:
        if cid is None:
            return ""
        try:
            return self._contragents.get(int(cid), _s(cid))
        except (ValueError, TypeError):
            return _s(cid)

    def account_name(self, aid: Any) -> str:
        if aid is None:
            return ""
        try:
            return self._accounts.get(int(aid), _s(aid))
        except (ValueError, TypeError):
            return _s(aid)

    def stream_name(self, sid: Any) -> str:
        if sid is None:
            return ""
        try:
            return self._streams.get(int(sid), _s(sid))
        except (ValueError, TypeError):
            return _s(sid)

    def organisation_name(self, oid: Any) -> str:
        if oid is None:
            return ""
        try:
            return self._organisations.get(int(oid), _s(oid))
        except (ValueError, TypeError):
            return _s(oid)

    def user_name(self, uid: Any) -> str:
        if uid is None:
            return ""
        try:
            return self._users.get(int(uid), _s(uid))
        except (ValueError, TypeError):
            return _s(uid)


# ──────────────────────────────────────────────────────────────────────────────
# Главный класс-оркестратор
# ──────────────────────────────────────────────────────────────────────────────

class SyncProcessor:
    def __init__(
        self,
        api: FinansistClient,
        sheets: GSheetsClient,
        db: SyncDatabase,
        sync_plan: bool = True,
        sync_fact: bool = True,
        force: bool = False,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        self._api = api
        self._sheets = sheets
        self._db = db
        self._sync_plan = sync_plan
        self._sync_fact = sync_fact
        self._force = force
        self._date_from = date_from
        self._date_to = date_to
        self._lookup = Lookup()
        self._lookup.load(api)
        # Набор годов, листы которых уже очищены в этом --force запуске
        self._force_cleared: set[int] = set()

    def run(self) -> dict:
        """Запускает синхронизацию. Возвращает статистику."""
        stats: dict = {
            "plan": {"total": 0, "new": 0, "updated": 0, "skipped": 0},
            "fact": {"total": 0, "new": 0, "updated": 0, "skipped": 0},
            "errors": [],
        }

        logger.info("=== Старт синхронизации Finansist → Google Sheets ===")

        if self._sync_plan:
            logger.info("--- ПЛАН: загрузка PaymentRequests ---")
            try:
                plan_ops = self._collect("plan")
                stats["plan"]["total"] = len(plan_ops)
                n, u, s = self._process(plan_ops, "plan")
                stats["plan"].update(new=n, updated=u, skipped=s)
            except Exception as e:
                logger.error("Ошибка синхронизации ПЛАН: %s", e)
                stats["errors"].append(f"ПЛАН: {e}")

        if self._sync_fact:
            logger.info("--- ФАКТ: загрузка Payments ---")
            try:
                fact_ops = self._collect("fact")
                stats["fact"]["total"] = len(fact_ops)
                n, u, s = self._process(fact_ops, "fact")
                stats["fact"].update(new=n, updated=u, skipped=s)
            except Exception as e:
                logger.error("Ошибка синхронизации ФАКТ: %s", e)
                stats["errors"].append(f"ФАКТ: {e}")

        stats["db"] = self._db.stats()
        logger.info("=== Синхронизация завершена ===")
        self._log_stats(stats)

        # Записываем строку в лог-вкладку таблицы
        try:
            self._sheets.write_sync_log(stats)
        except Exception as e:
            logger.warning("Не удалось записать лог в таблицу: %s", e)

        return stats

    def _collect(self, op_type: str) -> list[dict]:
        """Получает и нормализует все операции заданного типа."""
        result = []
        try:
            if op_type == "plan":
                raw_iter = self._api.get_payment_requests(
                    date_from=self._date_from,
                    date_to=self._date_to,
                )
                def normalizer(r):
                    return normalize_payment_request(r, self._lookup)
            else:
                raw_iter = self._api.get_payments(
                    date_from=self._date_from,
                    date_to=self._date_to,
                )
                def normalizer(r):
                    return normalize_payment(r, self._lookup)

            for raw in raw_iter:
                try:
                    op = normalizer(raw)
                    if op["year"] > 0:
                        result.append(op)
                    else:
                        logger.debug("Пропущена операция без даты: id=%s", raw.get("id"))
                except Exception as e:
                    logger.warning(
                        "Ошибка нормализации id=%s: %s", raw.get("id"), e
                    )
        except Exception as e:
            logger.error("Ошибка получения %s операций: %s", op_type, e)
            raise

        # Сортировка от новых к старым
        result.sort(key=lambda x: x.get("date", ""), reverse=True)
        logger.info("%s: нормализовано %d операций", op_type.upper(), len(result))
        return result

    def _process(self, operations: list[dict], op_type: str) -> tuple[int, int, int]:
        """Группирует по годам и записывает в Google Sheets."""
        by_year: dict[int, list[dict]] = defaultdict(list)
        for op in operations:
            by_year[op["year"]].append(op)

        total_new = total_updated = total_skipped = 0
        for year in sorted(by_year.keys()):
            n, u, s = self._sync_year(by_year[year], op_type, year)
            total_new += n
            total_updated += u
            total_skipped += s

        return total_new, total_updated, total_skipped

    def _sync_year(
        self, ops: list[dict], op_type: str, year: int
    ) -> tuple[int, int, int]:
        """Синхронизирует операции за один год на одну вкладку."""
        ws = self._sheets.get_or_create_sheet(year, HEADER)

        # При --force очищаем лист и SQLite, пишем всё с нуля
        if self._force:
            # Очищаем лист только один раз за весь запуск (план+факт пишутся на один лист)
            if year not in self._force_cleared:
                logger.info("Год %d: FORCE — очистка листа...", year)
                ws.clear()
                # Сжимаем до минимума чтобы не превышать лимит 10M ячеек Google Sheets
                ws.resize(rows=1, cols=len(HEADER))
                ws.append_row(HEADER, value_input_option="RAW")
                self._force_cleared.add(year)
            self._db.clear_year(year, op_type)

            new_rows = [operation_to_row(op) for op in ops]
            if new_rows:
                self._sheets.append_rows_batch(ws, new_rows)
                db_records = [
                    {
                        "operation_id": row[0],
                        "operation_type": op_type,
                        "year": year,
                        "last_status": row[STATUS_NAME_IDX],
                    }
                    for row in new_rows
                ]
                self._db.mark_synced_batch(db_records)
            logger.info("Год %d [%s]: записано %d строк", year, op_type, len(new_rows))
            return len(new_rows), 0, 0

        # Инкрементальный режим
        try:
            all_values = ws.get_all_values()
        except Exception as e:
            logger.warning("Не удалось прочитать вкладку %d: %s", year, e)
            all_values = [HEADER]

        # Индекс: (id, тип) → номер строки
        # Колонка A = ID (индекс 0), колонка C = Тип (индекс 2)
        existing: dict[tuple[str, str], int] = {}
        for row_idx, row in enumerate(all_values[1:], start=2):
            if row and len(row) >= 3:
                existing[(str(row[0]), str(row[2]))] = row_idx

        new_rows: list[list[str]] = []
        update_pairs: list[tuple[int, list[str]]] = []
        skipped = 0

        for op in ops:
            op_id = str(op["id"])
            key = (op_id, op_type)
            row_data = operation_to_row(op)
            current_status = op.get("status_name", "")

            if key in existing:
                db_status = self._db.get_status(op_id, op_type)
                if db_status == current_status:
                    skipped += 1
                    continue
                update_pairs.append((existing[key], row_data))
                self._db.mark_synced(op_id, op_type, year, last_status=current_status)
            else:
                new_rows.append(row_data)

        if new_rows:
            self._sheets.append_rows_batch(ws, new_rows)
            db_records = [
                {
                    "operation_id": row[0],
                    "operation_type": op_type,
                    "year": year,
                    "last_status": row[STATUS_NAME_IDX],
                }
                for row in new_rows
            ]
            self._db.mark_synced_batch(db_records)

        if update_pairs:
            self._sheets.update_rows_batch(ws, update_pairs)

        logger.info(
            "Год %d [%s]: +%d новых, ~%d обновлено, -%d пропущено",
            year, op_type, len(new_rows), len(update_pairs), skipped,
        )
        return len(new_rows), len(update_pairs), skipped

    @staticmethod
    def _log_stats(stats: dict):
        for key, label in [("plan", "ПЛАН"), ("fact", "ФАКТ")]:
            s = stats.get(key, {})
            logger.info(
                "%s: получено=%d | новых=%d | обновлено=%d | пропущено=%d",
                label, s.get("total", 0), s.get("new", 0),
                s.get("updated", 0), s.get("skipped", 0),
            )
