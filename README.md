# azure-data-collector

## What is it?

azure-data-collector is a Python client package for uploading events to Azure Monitor/Log Analytics workspace using the data collector api.

## How do I use it?

```python
    from azuredatacollector.datacollector import DataCollectorClient

    # Setup client
    client = DataCollectorClient(
        WORKSPACE_ID, SHARED_KEY
    )

    # Upload data without proxy
    test_data = [{"col": "row1"}, {"col": "row2"}, {"col": "row3"}]
    client.post_data(test_data, "TestLATable")

    # Upload data with proxy
    client.proxies = {"http": "http://127.0.0.1:8080"}
    metric = client.post_data(test_data, "TestLATable")
```

## Reference
https://learn.microsoft.com/en-us/azure/azure-monitor/logs/data-collector-api
