"""
FastAPI router for Document Storage (NoSQL) operations.
"""

from typing import Any, Optional, cast

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from au_sys_storage.interfaces.base_document_provider import IDocumentProvider
from au_sys_storage.routers._deps import get_document_provider

router = APIRouter()


def get_model_class(provider: IDocumentProvider, model_name: str) -> type[Any]:
    """Helper to find the model class by name in the provider's registry."""
    # Assuming the provider has a document_models list (as AsyncMongoDBProvider does)
    models = getattr(provider, "document_models", [])
    for model in models:
        if model.__name__ == model_name:
            return cast(type[Any], model)
    raise HTTPException(status_code=404, detail=f"Document model '{model_name}' not found")


class FindManyRequest(BaseModel):
    query: dict[str, Any]
    limit: int = 0
    skip: int = 0
    sort: Optional[Any] = None


@router.get("/document/models", response_model=list[str])
async def list_document_models(
    provider: IDocumentProvider = Depends(get_document_provider),
) -> list[str]:
    """List all registered document model names."""
    models = getattr(provider, "document_models", [])
    return [model.__name__ for model in models]


@router.post("/document/{model_name}", status_code=status.HTTP_201_CREATED)
async def insert_document(
    model_name: str,
    data: dict[str, Any],
    provider: IDocumentProvider = Depends(get_document_provider),
) -> dict[str, Any]:
    """Insert a single document."""
    model_class = get_model_class(provider, model_name)
    try:
        # Create instance of the model (assuming it's a Pydantic/Beanie model)
        doc = model_class(**data)
        result = await provider.insert_one(doc)
        return {"success": True, "data": result.model_dump() if hasattr(result, "model_dump") else result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Insert failed: {str(e)}")


@router.post("/document/{model_name}/many", status_code=status.HTTP_201_CREATED)
async def insert_many_documents(
    model_name: str,
    data: list[dict[str, Any]],
    provider: IDocumentProvider = Depends(get_document_provider),
) -> dict[str, Any]:
    """Insert multiple documents."""
    model_class = get_model_class(provider, model_name)
    try:
        docs = [model_class(**d) for d in data]
        results = await provider.insert_many(docs)
        return {
            "success": True,
            "count": len(results),
            "data": [r.model_dump() if hasattr(r, "model_dump") else r for r in results],
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Insert many failed: {str(e)}")


@router.post("/document/{model_name}/find_one")
async def find_one_document(
    model_name: str,
    query: dict[str, Any],
    provider: IDocumentProvider = Depends(get_document_provider),
) -> Optional[dict[str, Any]]:
    """Find a single document matching the query."""
    model_class = get_model_class(provider, model_name)
    doc = await provider.find_one(model_class, query)
    if doc:
        return cast(dict[str, Any], doc.model_dump() if hasattr(doc, "model_dump") else doc)
    return None


@router.post("/document/{model_name}/find_many")
async def find_many_documents(
    model_name: str,
    request: FindManyRequest,
    provider: IDocumentProvider = Depends(get_document_provider),
) -> list[Any]:
    """Find multiple documents matching the query."""
    model_class = get_model_class(provider, model_name)
    results = await provider.find_many(
        model_class,
        request.query,
        limit=request.limit,
        skip=request.skip,
        sort=request.sort,
    )
    return [r.model_dump() if hasattr(r, "model_dump") else r for r in results]


@router.delete("/document/{model_name}/many")
async def delete_many_documents(
    model_name: str,
    query: dict[str, Any],
    provider: IDocumentProvider = Depends(get_document_provider),
) -> dict[str, Any]:
    """Delete multiple documents matching the query."""
    model_class = get_model_class(provider, model_name)
    count = await provider.delete_many(model_class, query)
    return {"success": True, "deleted_count": count}


@router.delete("/document/{model_name}/{doc_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_document(
    model_name: str,
    doc_id: str,
    provider: IDocumentProvider = Depends(get_document_provider),
) -> None:
    """Delete a document by ID."""
    model_class = get_model_class(provider, model_name)
    # Beanie specific ID lookup or standard dict lookup
    # Beanie uses .get(id)
    if hasattr(model_class, "get"):
        doc = await model_class.get(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"Document '{doc_id}' not found")
        await provider.delete_one(doc)
    else:
        # Fallback for non-Beanie providers
        # This is a bit tricky if we don't have a standardized ID field name.
        # Assuming "_id" or "id" for now.
        doc = await provider.find_one(model_class, {"_id": doc_id})
        if not doc:
            doc = await provider.find_one(model_class, {"id": doc_id})
        if not doc:
            raise HTTPException(status_code=404, detail=f"Document '{doc_id}' not found")
        await provider.delete_one(doc)


@router.patch("/document/{model_name}/{doc_id}")
async def update_document(
    model_name: str,
    doc_id: str,
    update_query: dict[str, Any],
    provider: IDocumentProvider = Depends(get_document_provider),
) -> dict[str, Any]:
    """Update a document by ID."""
    model_class = get_model_class(provider, model_name)
    if hasattr(model_class, "get"):
        doc = await model_class.get(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"Document '{doc_id}' not found")
        result = await provider.update_one(doc, update_query)
        return cast(dict[str, Any], result.model_dump() if hasattr(result, "model_dump") else result)
    # Fallback
    doc = await provider.find_one(model_class, {"_id": doc_id})
    if not doc:
        doc = await provider.find_one(model_class, {"id": doc_id})
    if not doc:
        raise HTTPException(status_code=404, detail=f"Document '{doc_id}' not found")
    result = await provider.update_one(doc, update_query)
    return cast(dict[str, Any], result.model_dump() if hasattr(result, "model_dump") else result)
