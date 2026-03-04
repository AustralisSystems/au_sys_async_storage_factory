import pytest
import asyncio
import os
from beanie import Document, init_beanie
from au_sys_storage.providers.beanie_sqlite_adapter import BeanieSQLiteAdapter
from pydantic import Field


class TestDoc(Document):
    name: str
    value: int = Field(default=0)

    class Settings:
        name = "test_docs"


@pytest.mark.asyncio
async def test_beanie_sqlite_adapter() -> None:
    db_path = "test_beanie.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    adapter = BeanieSQLiteAdapter(db_path=db_path)
    await adapter.initialize(document_models=[TestDoc])

    # Test insert
    doc = TestDoc(name="test1", value=10)
    await adapter.insert_one(doc)
    assert doc.id is not None

    # Test find_one
    found = await adapter.find_one(TestDoc, {"name": "test1"})
    assert found is not None
    assert found.name == "test1"
    assert found.value == 10

    # Test find_many
    docs = await adapter.find_many(TestDoc, {"value": {"$gt": 5}})
    assert len(docs) == 1

    # Test update
    doc.value = 20
    await adapter.update_one(doc, {})
    found_updated = await adapter.find_one(TestDoc, {"name": "test1"})
    assert found_updated is not None
    assert found_updated.value == 20

    # Test delete
    await adapter.delete_one(doc)
    found_deleted = await adapter.find_one(TestDoc, {"name": "test1"})
    assert found_deleted is None

    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    asyncio.run(test_beanie_sqlite_adapter())
