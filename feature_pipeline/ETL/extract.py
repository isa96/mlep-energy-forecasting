import datetime
from json import JSONDecodeError
from typing import Any, Dict, Tuple, Optional

import pandas as pd
import requests

from yarl import URL
from .utils import get_logger


logger = get_logger(__name__)


def from_api(
    export_end_refrence_datetime: Optional[datetime.datetime]=None,
    days_delay: int = 15,
    days_export: int = 30,
    url: str = "https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"
    ) -> Optional[Tuple[pd.DataFrame, Dict[str, Any]]]:
    """
    Extract data from the DK energy consumption API.
    Args: 
        export_end_refrence_datetime: Tanggal refrensi akhir dari jendela ekspor, jika tidak ada waktu saat ini
        days_delay: data yang tertunda/jeda
        days_export: jumlah hari untuk mengekspor
        URL: url api
    return:
        Tuple pandas dataframe yg berisi data yg diekspor dan kamus data
    """
    if export_end_refrence_datetime is None:
        export_end_refrence_datetime = datetime.datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        )
    else:
        export_end_refrence_datetime = export_end_refrence_datetime.replace(
            minute=0, second=0, microsecond=0
        )
    export_end = export_end_refrence_datetime - datetime.timedelta(days=days_delay)
    export_start = export_end_refrence_datetime - datetime.timedelta(days=days_delay + days_export)

    # Query API
    query_params = {
        "offset": 0,
        "sort": "HourUTC",
        "timezone": "utc",
        "start": export_start.strftime("%Y-%m-%dT%H:%M"),
        "end": export_end.strftime("%Y-%m-%dT%H:%M")
    }

    url = URL(url) % query_params
    url = str(url)
    logger.info(f"Requesting data from API with URL: {url}")
    response = requests.get(url)
    logger.info(f"Response received from API with status code {response.status_code}")

    # parse API response
    try:
        response = response.json()
    except JSONDecodeError:
        logger.error(f"Response status: {response.status_code}")
        return None

    records = response["records"]
    records = pd.DataFrame.from_records(records)

    # prepare metadata
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    metadata = {
        "days_delay": days_delay,
        "days_export": days_export,
        "url": url,
        "export_datetime_utc_start": export_start.strftime(datetime_format),
        "export_datetime_utc_end": export_end.strftime(datetime_format),
        "datetime_format": datetime_format,
    }

    return records, metadata