"""
FastAPI router for DataManager operations (file CRUD, streaming, checksums).
"""

import json
from typing import Any, Optional, Union

from fastapi import APIRouter, Depends, File, HTTPException, Query, Response, UploadFile, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from au_sys_storage.routers._deps import get_data_manager
from au_sys_storage.shared.services.data.data_manager import DataManager

router = APIRouter()


class CsvWriteRequest(BaseModel):
    rows: Union[list[dict[str, Any]], list[list[Any]]]
    fieldnames: Optional[list[str]] = None
    encoding: str = "utf-8"
    create_parents: bool = True


class NdjsonWriteRequest(BaseModel):
    records: list[dict[str, Any]]
    encoding: str = "utf-8"
    create_parents: bool = True


# Sub-group A: Core File I/O


@router.post("/files", status_code=status.HTTP_201_CREATED)
async def upload_file(
    path: str = Query(..., description="Relative path to save the file"),
    file: UploadFile = File(...),
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Upload a file to the data directory."""
    content = await file.read()
    await data_manager.write_file(path, content.decode("utf-8") if isinstance(content, bytes) else content)
    return {"success": True, "path": path, "bytes_transferred": len(content)}


@router.post("/files/json", status_code=status.HTTP_201_CREATED)
async def write_json_file(
    path: str,
    data: Any,
    indent: int = 4,
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Write a JSON file."""
    await data_manager.write_json(path, data, indent=indent)
    return {"success": True, "path": path}


@router.get("/files", response_model=list[str])
async def list_files(
    pattern: str = "**/*",
    data_manager: DataManager = Depends(get_data_manager),
) -> list[str]:
    """List files in the data directory matching a pattern."""
    paths = await data_manager.list_files(pattern)
    return [str(p) for p in paths]


@router.get("/files/{path:path}")
async def read_file(
    path: str,
    data_manager: DataManager = Depends(get_data_manager),
) -> Response:
    """Read a file as text."""
    try:
        content = await data_manager.read_file(path)
        return Response(content=content, media_type="text/plain")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


@router.get("/files/{path:path}/bytes")
async def read_file_bytes(
    path: str,
    data_manager: DataManager = Depends(get_data_manager),
) -> Response:
    """Read raw bytes of a file."""
    try:
        content = await data_manager.read_bytes(path)
        return Response(content=content, media_type="application/octet-stream")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


@router.get("/files/{path:path}/json")
async def read_json_file(
    path: str,
    data_manager: DataManager = Depends(get_data_manager),
) -> Any:
    """Read and return parsed JSON from a file."""
    try:
        data = await data_manager.read_json(path)
        return data
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in '{path}': {str(e)}")


@router.delete("/files/{path:path}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_file(
    path: str,
    data_manager: DataManager = Depends(get_data_manager),
) -> None:
    """Delete a file from the data directory."""
    try:
        await data_manager.delete_file(path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


@router.get("/files/{path:path}/exists")
async def file_exists(
    path: str,
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Check if a file or directory exists."""
    exists = await data_manager.exists(path)
    return {"path": path, "exists": exists}


# Sub-group B: Checksums and Integrity


@router.get("/files/{path:path}/checksum")
async def get_file_checksum(
    path: str,
    algorithm: str = "sha256",
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Compute file checksum."""
    try:
        digest = await data_manager.checksum(path, algorithm=algorithm)
        return {"path": path, "algorithm": algorithm, "checksum": digest}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


@router.post("/files/{path:path}/verify")
async def verify_file_checksum(
    path: str,
    expected: str,
    algorithm: str = "sha256",
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Verify file checksum."""
    try:
        ok = await data_manager.verify_checksum(path, expected, algorithm=algorithm)
        return {"path": path, "valid": ok}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


# Sub-group C: Streaming I/O


@router.get("/files/{path:path}/stream")
async def stream_read_file(
    path: str,
    chunk_size: int = 1024 * 64,
    data_manager: DataManager = Depends(get_data_manager),
) -> StreamingResponse:
    """Stream read a file."""
    try:
        return StreamingResponse(
            data_manager.stream_read(path, chunk_size=chunk_size),
            media_type="application/octet-stream",
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


@router.post("/files/{path:path}/stream")
async def stream_write_file(
    path: str,
    data_manager: DataManager = Depends(get_data_manager),
) -> None:
    """Stream write a file from request body."""
    raise HTTPException(status_code=501, detail="Stream write via REST is not supported in this endpoint")


# Sub-group D: Staging and Lifecycle


@router.post("/staging/files", status_code=status.HTTP_201_CREATED)
async def create_staging_file(
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Create a temporary file in the staging directory."""
    path = await data_manager.create_staging_file(suffix=suffix, prefix=prefix)
    return {"success": True, "path": str(path)}


@router.post("/staging/cleanup")
async def cleanup_staging(
    older_than_seconds: int = 3600,
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Clean up files in the staging directory."""
    result = await data_manager.cleanup_staging(older_than_seconds=older_than_seconds)
    return result


@router.post("/files/{path:path}/archive")
async def archive_file(
    path: str,
    archive_subdir: str = "archive",
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Move a file to the archive location."""
    try:
        dest = await data_manager.archive(path, archive_subdir=archive_subdir)
        return {"success": True, "archived_at": str(dest)}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


# Sub-group E: Directory Lifecycle


@router.post("/directories", status_code=status.HTTP_201_CREATED)
async def make_directory(
    path: str = Query(..., description="Relative path to create"),
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Create a directory."""
    abs_path = await data_manager.make_dir(path)
    return {"success": True, "path": path, "abs_path": str(abs_path)}


@router.delete("/directories/{path:path}")
async def remove_directory(
    path: str,
    recursive: bool = False,
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Remove a directory."""
    success = await data_manager.remove_dir(path, recursive=recursive)
    if not success:
        raise HTTPException(status_code=400, detail=f"Failed to remove directory '{path}'")
    return {"success": True}


# Sub-group F: Transformation / Serialisation


@router.get("/files/{path:path}/csv")
async def read_csv_file(
    path: str,
    has_header: bool = True,
    encoding: str = "utf-8",
    data_manager: DataManager = Depends(get_data_manager),
) -> Any:
    """Read and parse a CSV file."""
    try:
        return await data_manager.read_csv(path, has_header=has_header, encoding=encoding)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


@router.post("/files/{path:path}/csv")
async def write_csv_file(
    path: str,
    request: CsvWriteRequest,
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Serialise rows to CSV and write to a file."""
    abs_path = await data_manager.write_csv(
        path,
        request.rows,
        fieldnames=request.fieldnames,
        encoding=request.encoding,
        create_parents=request.create_parents,
    )
    return {"success": True, "path": path, "abs_path": str(abs_path)}


@router.get("/files/{path:path}/ndjson")
async def read_ndjson_file(
    path: str,
    encoding: str = "utf-8",
    data_manager: DataManager = Depends(get_data_manager),
) -> Any:
    """Read and parse an NDJSON file."""
    try:
        return await data_manager.read_ndjson(path, encoding=encoding)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File '{path}' not found")


@router.post("/files/{path:path}/ndjson")
async def write_ndjson_file(
    path: str,
    request: NdjsonWriteRequest,
    data_manager: DataManager = Depends(get_data_manager),
) -> dict[str, Any]:
    """Serialise objects as NDJSON and write to a file."""
    abs_path = await data_manager.write_ndjson(
        path,
        request.records,
        encoding=request.encoding,
        create_parents=request.create_parents,
    )
    return {"success": True, "path": path, "abs_path": str(abs_path)}
