"""
Клиент для работы с Google Sheets через gspread.
Поддерживает пакетную запись, автосоздание вкладок по годам
и защиту от превышения лимитов Google API.
"""

import logging
import time
from typing import Any

import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

# Скоупы, необходимые для чтения и записи в Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# Лимиты Google Sheets API (квота по умолчанию)
BATCH_WRITE_DELAY = 1.1       # секунды между пакетными записями
MAX_ROWS_PER_BATCH = 1000     # строк за один вызов values_append/update
RATE_LIMIT_RETRIES = 5        # попыток при 429/503
RATE_LIMIT_BACKOFF = 60       # секунд ожидания при rate limit


class GSheetsClient:
    def __init__(
        self,
        spreadsheet_id: str,
        credentials_path: str,
    ):
        self.spreadsheet_id = spreadsheet_id
        creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        self._gc = gspread.authorize(creds)
        self._spreadsheet = None
        self._sheet_cache: dict[str, gspread.Worksheet] = {}

    @property
    def spreadsheet(self) -> gspread.Spreadsheet:
        if self._spreadsheet is None:
            try:
                self._spreadsheet = self._gc.open_by_key(self.spreadsheet_id)
                logger.info("Открыта таблица: %s", self._spreadsheet.title)
            except SpreadsheetNotFound:
                raise RuntimeError(
                    f"Таблица {self.spreadsheet_id} не найдена. "
                    "Убедитесь, что Service Account добавлен в редакторы."
                )
        return self._spreadsheet

    def _retry_api(self, func, *args, **kwargs):
        """Обёртка с повторными попытками при 429/503."""
        for attempt in range(RATE_LIMIT_RETRIES):
            try:
                return func(*args, **kwargs)
            except APIError as e:
                if e.response.status_code in (429, 503) and attempt < RATE_LIMIT_RETRIES - 1:
                    wait = RATE_LIMIT_BACKOFF * (attempt + 1)
                    logger.warning(
                        "Rate limit (HTTP %d). Ожидаю %d сек (попытка %d/%d)...",
                        e.response.status_code,
                        wait,
                        attempt + 1,
                        RATE_LIMIT_RETRIES,
                    )
                    time.sleep(wait)
                else:
                    raise

    def get_or_create_sheet(self, year: int, header_columns: list[str]) -> gspread.Worksheet:
        """
        Возвращает вкладку с именем года (строка).
        Если вкладки нет — создаёт её и записывает заголовок.
        """
        sheet_name = str(year)

        if sheet_name in self._sheet_cache:
            return self._sheet_cache[sheet_name]

        # Ищем существующую
        existing = {ws.title: ws for ws in self.spreadsheet.worksheets()}
        if sheet_name in existing:
            ws = existing[sheet_name]
            logger.info("Найдена вкладка '%s'", sheet_name)
        else:
            # Создаём новую вкладку
            ws = self._retry_api(
                self.spreadsheet.add_worksheet,
                title=sheet_name,
                rows=5000,
                cols=len(header_columns) + 5,
            )
            logger.info("Создана новая вкладка '%s'", sheet_name)
            # Записываем заголовок
            self._retry_api(ws.update, "A1", [header_columns])
            time.sleep(BATCH_WRITE_DELAY)

        self._sheet_cache[sheet_name] = ws
        return ws

    def get_existing_ids_in_sheet(
        self, year: int, id_column_index: int = 0
    ) -> set[str]:
        """
        Читает все значения из колонки ID на указанной вкладке.
        Используется для проверки существующих записей без обращения к SQLite.
        """
        sheet_name = str(year)
        existing = {ws.title: ws for ws in self.spreadsheet.worksheets()}
        if sheet_name not in existing:
            return set()

        ws = existing[sheet_name]
        col_letter = chr(ord("A") + id_column_index)
        values = self._retry_api(ws.col_values, id_column_index + 1)
        # Первая строка — заголовок, пропускаем
        return set(str(v) for v in values[1:] if v)

    def append_rows_batch(
        self,
        worksheet: gspread.Worksheet,
        rows: list[list[Any]],
    ) -> int:
        """
        Добавляет строки пакетами. Возвращает количество записанных строк.
        """
        if not rows:
            return 0

        written = 0
        for i in range(0, len(rows), MAX_ROWS_PER_BATCH):
            chunk = rows[i : i + MAX_ROWS_PER_BATCH]
            self._retry_api(
                worksheet.append_rows,
                chunk,
                value_input_option="USER_ENTERED",
                insert_data_option="INSERT_ROWS",
            )
            written += len(chunk)
            logger.info(
                "Записано %d строк в '%s' (всего: %d/%d)",
                len(chunk),
                worksheet.title,
                written,
                len(rows),
            )
            if i + MAX_ROWS_PER_BATCH < len(rows):
                time.sleep(BATCH_WRITE_DELAY)

        return written

    def update_rows_batch(
        self,
        worksheet: gspread.Worksheet,
        row_updates: list[tuple[int, list[Any]]],
    ) -> int:
        """
        Обновляет конкретные строки по номеру.
        row_updates: список (row_number_1based, row_values)
        """
        if not row_updates:
            return 0

        batch_data = []
        for row_num, values in row_updates:
            end_col = chr(ord("A") + len(values) - 1)
            batch_data.append({
                "range": f"A{row_num}:{end_col}{row_num}",
                "values": [values],
            })

        # Отправляем пакетами по 500 диапазонов
        updated = 0
        for i in range(0, len(batch_data), 500):
            chunk = batch_data[i : i + 500]
            self._retry_api(
                worksheet.batch_update,
                chunk,
                value_input_option="USER_ENTERED",
            )
            updated += len(chunk)
            if i + 500 < len(batch_data):
                time.sleep(BATCH_WRITE_DELAY)

        return updated

    def list_sheet_names(self) -> list[str]:
        """Возвращает список названий всех вкладок."""
        return [ws.title for ws in self.spreadsheet.worksheets()]

    def write_sync_log(self, stats: dict) -> None:
        """
        Записывает строку в вкладку 'Лог' с датой/временем и итогами синхронизации.
        Создаёт вкладку если её нет.
        """
        from datetime import datetime, timezone

        LOG_SHEET = "Лог"
        LOG_HEADER = [
            "Дата", "Время (UTC)", "ПЛАН получено", "ПЛАН новых",
            "ПЛАН обновлено", "ФАКТ получено", "ФАКТ новых",
            "ФАКТ обновлено", "Ошибки",
        ]

        existing = {ws.title: ws for ws in self.spreadsheet.worksheets()}
        if LOG_SHEET not in existing:
            ws = self._retry_api(
                self.spreadsheet.add_worksheet,
                title=LOG_SHEET,
                rows=1000,
                cols=len(LOG_HEADER),
            )
            self._retry_api(ws.update, "A1", [LOG_HEADER])
            ws.format("A1:I1", {"textFormat": {"bold": True}})
            time.sleep(BATCH_WRITE_DELAY)
            logger.info("Создана вкладка '%s'", LOG_SHEET)
        else:
            ws = existing[LOG_SHEET]
            # Обновляем заголовок если он устарел (старый формат — 1 колонка вместо 2)
            current_header = ws.row_values(1)
            if current_header != LOG_HEADER:
                self._retry_api(ws.update, "A1", [LOG_HEADER])
                ws.format("A1:I1", {"textFormat": {"bold": True}})
                logger.info("Заголовок лога обновлён")

        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M:%S")
        p = stats.get("plan", {})
        f = stats.get("fact", {})
        errors = "; ".join(stats.get("errors", [])) or "—"

        row = [
            date_str, time_str,
            p.get("total", 0), p.get("new", 0), p.get("updated", 0),
            f.get("total", 0), f.get("new", 0), f.get("updated", 0),
            errors,
        ]
        self._retry_api(ws.append_rows, [row], value_input_option="USER_ENTERED")
        logger.info("Лог синхронизации записан: %s %s", date_str, time_str)
