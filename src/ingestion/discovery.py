from pathlib import Path

REQUIRED_FILES = [
    "customers.csv",
    "orders.csv",
    "order_items.csv",
    "products.csv",
    "sellers.csv",
    "payments.csv",
    "reviews.csv",
]


def dataset_discovery(source_dir: str = "/opt/airflow/data/source") -> list[str]:
    """
    Validate required source datasets.

    Args:
        source_dir: Source data directory.

    Returns:
        List of validated file paths.
    """

    validated_paths = []

    for file_name in REQUIRED_FILES:
        file_path = Path(source_dir) / file_name

        if not file_path.exists():
            raise FileNotFoundError(f"Required source file missing: {file_name}")
        if file_path.stat().st_size == 0:
            raise ValueError(f"Required source file is empty: {file_name}")

        validated_paths.append(str(file_path))

    return validated_paths
