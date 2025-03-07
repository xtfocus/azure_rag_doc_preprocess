import asyncio
from typing import Any, Dict, List

import httpx
from loguru import logger


async def check_pii_async(
    documents: List[Dict[str, Any]],
    service_endpoint: str,
    retries: int = 4,
    backoff_factor: float = 2,
) -> Dict:
    """
    Retries up to 3 times if an exception occurs (httpx.HTTPStatusError or httpx.RequestError).
    Exponential backoff (1s → 2s → 4s) to avoid hammering the server.
    Raises the last exception if all retries fail.
    """
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    for attempt in range(retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    service_endpoint, json=documents, headers=headers
                )
                response.raise_for_status()
                return response.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            if attempt < retries - 1:
                wait_time = backoff_factor * (2**attempt)  # Exponential backoff
                logger.warning(
                    f"Failed to send PII scanning request Retrying after {wait_time}s"
                )
                await asyncio.sleep(wait_time)
            else:

                logger.error(f"Failed to send PII scanning request.")
                raise e  # Raise the last exception if all retries fail


def check_sensitive_information(pii_scan_result: Dict[str, Any]):
    detected_data = []

    for entry in pii_scan_result.get("data", []):
        pii_result = entry.get("pii_result", {})
        entities = pii_result.get("entities", [])

        if entities:  # If entities list is not empty
            for entity in entities:
                detected_data.append(
                    {
                        "text": entity.get("text"),
                        "category": entity.get("category"),
                    }
                )

    return detected_data
