"""
NYC taxi data pipeline — REST API source.
Loads paginated JSON from the Data Engineering Zoomcamp API (1,000 records per page;
stops when an empty page is returned).
"""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

# Base URL for the NYC taxi API; no auth required
BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources from the NYC taxi REST API (offset pagination, 1000 per page)."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": BASE_URL,
            "paginator": {
                "type": "offset",
                "limit": 50000,
                "offset": 0,
                "offset_param": "offset",
                "limit_param": "limit",
                "total_path": None,
                # During development, cap how much we load so runs finish quickly.
                # This loads at most 10 * 1000 = 10,000 rows.
                "maximum_offset": 100000,
                "stop_after_empty_page": True,
            },
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    # Root path: API returns a bare JSON array with ?limit=&offset=
                    "path": "",
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nytaxi_pipeline",   # or just remove this line to use the default
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)
