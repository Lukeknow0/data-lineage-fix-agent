"""Trusted fixture DAG field mapping used by the LineageTX demo."""

FIELD_MAPPING = {
    "customer_id": "customer_id",
    "order_id": "order_id",
    "total_amount": "total_amount",
}


def export_columns() -> tuple[str, ...]:
    return tuple(FIELD_MAPPING.values())
