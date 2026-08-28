from urllib.parse import quote

from fastapi import HTTPException


def parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    """Parse one HTTP byte range into inclusive start/end bounds."""
    if not range_header:
        return 0, file_size - 1
    try:
        value = range_header.replace("bytes=", "").strip()
        start_str, end_str = value.split("-")
        if start_str == "":
            length = int(end_str)
            start = file_size - length
            end = file_size - 1
        elif end_str == "":
            start = int(start_str)
            end = file_size - 1
        else:
            start = int(start_str)
            end = int(end_str)
    except Exception:
        raise HTTPException(
            status_code=416,
            detail="Invalid Range header",
            headers={"Content-Range": f"bytes */{file_size}"},
        )
    if start < 0:
        start = 0
    if end >= file_size:
        end = file_size - 1
    if end < start:
        raise HTTPException(
            status_code=416,
            detail="Requested Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )
    return start, end


def content_disposition(file_name: str, disposition: str = "inline") -> str:
    ascii_fallback = file_name.encode("ascii", "ignore").decode("ascii").replace('"', "").strip() or "file"
    return f"{disposition}; filename=\"{ascii_fallback}\"; filename*=UTF-8''{quote(file_name, safe='')}"


def build_stream_headers(
    mime_type: str,
    file_name: str,
    requested_length: int,
    range_header: str,
    start: int,
    end: int,
    file_size: int,
) -> tuple[dict[str, str], int]:
    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": content_disposition(file_name),
        "Accept-Ranges": "bytes",
        "Content-Length": str(requested_length),
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    }
    status = 200
    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        status = 206
    return headers, status
