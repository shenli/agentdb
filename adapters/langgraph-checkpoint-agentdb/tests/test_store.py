from langgraph_checkpoint_agentdb import AgentDBStore


class FakeClient:
    def __init__(self):
        self.writes = []
        self.searches = []

    def write_record(self, tenant_id, **body):
        self.writes.append((tenant_id, body))
        return {"record_id": "rec_1", **body}

    def search_records(self, tenant_id, **body):
        self.searches.append((tenant_id, body))
        return [
            {
                "record_id": "rec_1",
                "metadata": {
                    "key": "incident-1",
                    "value": {"status": "open"},
                    "metadata": {"owner": "ops"},
                },
            }
        ]


def test_put_maps_store_item_to_searchable_record():
    client = FakeClient()
    store = AgentDBStore(client, tenant_id="ten_1")

    result = store.put(("incidents",), "incident-1", {"status": "open"}, metadata={"owner": "ops"}, tags=["open"])

    assert result["record_id"] == "rec_1"
    tenant_id, body = client.writes[0]
    assert tenant_id == "ten_1"
    assert body["namespace"] == ["langgraph", "store", "incidents"]
    assert "key:incident-1" in body["tags"]
    assert "open" in body["tags"]
    assert body["metadata"]["value"] == {"status": "open"}


def test_search_maps_query_and_records_to_store_items():
    client = FakeClient()
    store = AgentDBStore(client, tenant_id="ten_1")

    rows = store.search(("incidents",), query="open", tags=["open"], limit=5)

    assert client.searches[0][1] == {
        "namespace": ["langgraph", "store", "incidents"],
        "text": "open",
        "tags": ["open"],
        "embedding": None,
        "limit": 5,
    }
    assert rows[0].namespace == ("langgraph", "store", "incidents")
    assert rows[0].key == "incident-1"
    assert rows[0].value == {"status": "open"}
    assert rows[0].metadata == {"owner": "ops"}
