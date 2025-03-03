from typing import Any, Dict, List

import httpx

from src.models import SensitiveInformationDetectedException


async def check_pii_async(
    documents: List[Dict[str, Any]], service_endpoint: str
) -> Dict:
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(service_endpoint, json=documents, headers=headers)
        response.raise_for_status()
        return response.json()


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

    if detected_data:
        raise SensitiveInformationDetectedException(detected_data)
