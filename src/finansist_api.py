"""
Клиент для работы с API Финансиста (rest.api.report.finance).

Документация: /n8n/reportfinance-api.md
Base URL: https://rest.api.report.finance/api/
Auth: заголовок 'apiKey' со значением ключа интеграции

Ключевые эндпоинты:
- GET /api/PaymentRequests  → ПЛАН (заявки на платеж)
- GET /api/Payments         → ФАКТ (фактические платежи)
- GET /api/Contragents      → контрагенты
- GET /api/Projects         → проекты
- GET /api/FactStreams       → статьи факта
- GET /api/Streams          → статьи (категории)
- GET /api/Accounts         → счета
"""

import logging
from typing import Any, Generator

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 100

# Маппинг статусов PaymentRequests
PAYMENT_REQUEST_STATUSES = {
    710: "Создана/На подписание",
    711: "На подписании",
    712: "Подписана",
    713: "Отозвана",
}


class FinansistAPIError(Exception):
    pass


class FinansistClient:
    """
    Клиент Finansist API.

    Args:
        base_url: базовый URL, напр. https://rest.api.report.finance
        api_key: ключ интеграции (создаётся в интерфейсе Финансиста)
        page_size: кол-во записей на страницу (API лимит = 100)
        timeout: таймаут HTTP-запроса в секундах
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self._headers = {
            "apiKey": api_key,
            "X-API-KEY": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._client = httpx.Client(
            headers=self._headers,
            timeout=timeout,
        )

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        reraise=True,
    )
    def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{self.base_url}/api/{path.lstrip('/')}"
        logger.debug("GET %s params=%s", url, params)
        try:
            response = self._client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise FinansistAPIError(
                f"HTTP {e.response.status_code} для {url}: {e.response.text[:500]}"
            ) from e

    def _paginate(
        self,
        endpoint: str,
        list_key: str,
        extra_params: dict | list | None = None,
    ) -> Generator[dict, None, None]:
        """
        Итерирует все страницы эндпоинта через offset-пагинацию.
        API лимит: 100 записей на страницу.
        extra_params может быть dict или list[tuple] (для повторяющихся ключей, напр. statusId).
        """
        offset = 0
        total_fetched = 0
        # Нормализуем в список кортежей чтобы поддерживать повторяющиеся ключи
        if isinstance(extra_params, dict):
            base_params: list[tuple] = list(extra_params.items())
        elif isinstance(extra_params, list):
            base_params = list(extra_params)
        else:
            base_params = []

        while True:
            params = base_params + [("offset", offset)]
            data = self._get(endpoint, params=params)

            # API может вернуть список напрямую или обёрнутый в объект
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get(list_key, [])
                if not items:
                    # Попробуем альтернативные ключи
                    for alt_key in ("data", "items", "result", "records"):
                        if alt_key in data and isinstance(data[alt_key], list):
                            items = data[alt_key]
                            logger.debug("Используем ключ '%s' для %s", alt_key, endpoint)
                            break
            else:
                items = []

            if not items:
                logger.info(
                    "%s: данные исчерпаны на offset=%d (получено всего: %d)",
                    endpoint, offset, total_fetched,
                )
                break

            for item in items:
                yield item

            total_fetched += len(items)
            logger.info(
                "%s: offset=%d, получено=%d, всего=%d",
                endpoint, offset, len(items), total_fetched,
            )

            if len(items) < self.page_size:
                break

            offset += self.page_size

    # ──────────────────────────────────────────────
    # Финансовые операции
    # ──────────────────────────────────────────────

    def get_payment_requests(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        project_id: int | None = None,
        organisation_id: int | None = None,
        status_ids: list[int] | None = None,
    ) -> Generator[dict, None, None]:
        """
        ПЛАН: заявки на платеж.
        GET /api/PaymentRequests

        Поля ответа: id, externalId, paymentDate, factStreamId, contragentId,
                     accountId, projectId, organisationId, statusId, comment,
                     paymentFor, paymentPurpose, sourcePaymentSum, isUrgent, tags

        statusId: 710=Создана/На подписание, 711=На подписании,
                  712=Подписана, 713=Отозвана

        ВАЖНО: API требует statusId обязательно. По умолчанию запрашиваем все статусы.
        httpx кодирует список как statusId=710&statusId=711&...
        """
        # API требует statusId — передаём все статусы если не задан конкретный
        ids = status_ids if status_ids is not None else list(PAYMENT_REQUEST_STATUSES.keys())

        # httpx принимает список значений для одного ключа через список кортежей
        params: list[tuple[str, Any]] = [("statusId", sid) for sid in ids]

        if date_from:
            params.append(("dateFrom", date_from))
        if date_to:
            params.append(("dateTo", date_to))
        if project_id:
            params.append(("projectId", project_id))
        if organisation_id:
            params.append(("organisationId", organisation_id))

        yield from self._paginate("PaymentRequests", "listPaymentRequest", params)

    def get_payments(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        project_id: int | None = None,
        organisation_id: int | None = None,
    ) -> Generator[dict, None, None]:
        """
        ФАКТ: фактические платежи.
        GET /api/Payments

        Поля ответа: id, externalId, date/paymentDate, amount/sum,
                     contragentId, accountId, projectId, organisationId,
                     streamId/factStreamId, comment, tags
        """
        extra: dict[str, Any] = {}
        if date_from:
            extra["dateFrom"] = date_from
        if date_to:
            extra["dateTo"] = date_to
        if project_id:
            extra["projectId"] = project_id
        if organisation_id:
            extra["organisationId"] = organisation_id

        yield from self._paginate("Payments", "listPayment", extra)

    # ──────────────────────────────────────────────
    # Справочники (для обогащения данных)
    # ──────────────────────────────────────────────

    def get_contragents(self) -> Generator[dict, None, None]:
        """GET /api/Contragents — контрагенты."""
        yield from self._paginate("Contragents", "listContragent")

    def get_projects(self, group_id: int | None = None) -> Generator[dict, None, None]:
        """GET /api/Projects — проекты."""
        extra: dict[str, Any] = {}
        if group_id is not None:
            extra["groupId"] = group_id
        yield from self._paginate("Projects", "listProject", extra)

    def get_project_groups(self) -> list[dict]:
        """GET /api/ProjectGroups — группы проектов."""
        return list(self._paginate("ProjectGroups", "listProjectGroup"))

    def get_fact_streams(self) -> list[dict]:
        """GET /api/FactStreams — статьи факта (категории)."""
        data = self._get("FactStreams")
        if isinstance(data, list):
            return data
        return data.get("listFactStream", []) if isinstance(data, dict) else []

    def get_streams(self) -> list[dict]:
        """GET /api/Streams — все статьи (категории)."""
        data = self._get("Streams")
        if isinstance(data, list):
            return data
        return data.get("listStream", []) if isinstance(data, dict) else []

    def get_accounts(self) -> list[dict]:
        """GET /api/Accounts — банковские счета."""
        return list(self._paginate("Accounts", "listAccount"))

    def get_organisations(self) -> list[dict]:
        """GET /api/Organisations — организации."""
        return list(self._paginate("Organisations", "listOrganisation"))

    def get_users(self) -> list[dict]:
        """GET /api/Users — пользователи системы (инициаторы заявок)."""
        data = self._get("Users")
        if isinstance(data, list):
            return data
        for key in ("listUser", "users", "data", "items", "result"):
            if isinstance(data, dict) and key in data:
                return data[key]
        return []

    def get_managers(self) -> list[dict]:
        """GET /api/Manager — менеджеры."""
        return list(self._paginate("Manager", "listManager"))

    # ──────────────────────────────────────────────
    # Утилита для отладки
    # ──────────────────────────────────────────────

    def discover(self, endpoint: str) -> dict | list:
        """
        Делает один запрос к эндпоинту и возвращает сырой ответ.
        Используй при первоначальной настройке для изучения структуры ответа.

        Пример:
            client.discover("Payments")
            client.discover("PaymentRequests")
        """
        result = self._get(endpoint, params={"offset": 0})
        if isinstance(result, dict):
            logger.info("Ключи ответа %s: %s", endpoint, list(result.keys()))
        elif isinstance(result, list) and result:
            logger.info("Ключи первого элемента %s: %s", endpoint, list(result[0].keys()))
        return result
