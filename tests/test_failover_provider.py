import pytest
import asyncio
import os
from beanie import Document
from storage.providers.beanie_sqlite_adapter import BeanieSQLiteAdapter
from storage.document_failover_provider import DocumentFailoverProvider
from pydantic import Field


class FailoverDoc(Document):
    name: str
    value: int = Field(default=0)

    class Settings:
        name = "failover_docs"


@pytest.mark.asyncio
async def test_document_failover_provider():
    primary_db = "primary_test.db"
    secondary_db = "secondary_test.db"

    for db in [primary_db, secondary_db]:
        if os.path.exists(db):
            os.remove(db)

    primary = BeanieSQLiteAdapter(db_path=primary_db)
    secondary = BeanieSQLiteAdapter(db_path=secondary_db)

    failover = DocumentFailoverProvider(primary=primary, secondary=secondary, failover_threshold=1)
    await failover.initialize(document_models=[FailoverDoc])

    # Test primary insert
    doc = FailoverDoc(name="p1", value=100)
    await failover.insert_one(doc)

    # Verify in primary
    found_p = await primary.find_one(FailoverDoc, {"name": "p1"})
    assert found_p is not None
    assert found_p.value == 100

    # Simulate primary failure (manually switch active provider)
    # In real world, it happens on Exception
    failover._active_provider = secondary
    failover._is_failing_over = True

    # Test secondary insert
    doc2 = FailoverDoc(name="s1", value=200)
    await failover.insert_one(doc2)

    # Verify in secondary
    found_s = await secondary.find_one(FailoverDoc, {"name": "s1"})
    assert found_s is not None
    assert found_s.value == 200

    # Verify primary doesn't have doc2
    found_p2 = await primary.find_one(FailoverDoc, {"name": "s1"})
    assert found_p2 is None

    # Cleanup
    for db in [primary_db, secondary_db]:
        if os.path.exists(db):
            os.remove(db)


if __name__ == "__main__":
    asyncio.run(test_document_failover_provider())
