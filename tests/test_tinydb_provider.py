import pytest
import asyncio
import os
from beanie import Document
from storage.providers.beanie_tinydb_adapter import BeanieTinyDBAdapter
from pydantic import Field


class TinyTestDoc(Document):
    name: str
    value: int = Field(default=0)

    class Settings:
        name = "tiny_docs"


@pytest.mark.asyncio
async def test_beanie_tinydb_adapter() -> None:
    db_path = "test_tiny.json"
    if os.path.exists(db_path):
        os.remove(db_path)

    adapter = BeanieTinyDBAdapter(db_path=db_path)  # type: ignore[abstract]
    await adapter.initialize(document_models=[TinyTestDoc])

    # Test insert
    doc = TinyTestDoc(name="t1", value=50)
    await adapter.insert_one(doc)
    assert doc.id is not None

    # Test find_one
    found = await adapter.find_one(TinyTestDoc, {"name": "t1"})
    assert found is not None
    assert found.name == "t1"

    # Test delete
    await adapter.delete_one(doc)
    found_deleted = await adapter.find_one(TinyTestDoc, {"name": "t1"})
    assert found_deleted is None

    adapter.close()

    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    asyncio.run(test_beanie_tinydb_adapter())
