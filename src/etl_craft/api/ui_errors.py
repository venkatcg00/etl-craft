"""Browser error states keep missing objects and denied objects indistinguishable."""

from urllib.parse import quote, urlsplit

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.exceptions import HTTPException

from etl_craft.api.browser_sessions import html_request, return_path
from etl_craft.api.ui_templates import render


def browser_error(request: Request, status: int, message: str = "") -> Response:
    """Render no secret exception detail on server failures and no existence hint on a 404."""
    server = status >= 500
    missing = status == 404
    values = {
        "title": "Request failed · etl-craft",
        "view": "error",
        "heading": "Server error"
        if server
        else "Not found or no access"
        if missing
        else "Request refused",
        "message": (
            "The server could not complete this request. Give the request id to your administrator."
        )
        if server
        else (
            "This page is unavailable or you do not have access. Ask your "
            "administrator, or return to Home."
        )
        if missing
        else message,
        "request_id": getattr(request.state, "request_id", "unknown") if server else None,
    }
    if request.headers.get("HX-Request") == "true" and not server:
        return render(request, "refusal.html", values, status)
    return render(request, "error.html", values, status)


def http_error(request: Request, error: HTTPException) -> Response:
    """Unauthenticated document requests preserve a same-site destination for sign-in."""
    if error.status_code == 307 and error.headers and "Location" in error.headers:
        return RedirectResponse(error.headers["Location"], 307)
    if html_request(request):
        if error.status_code == 401:
            destination = request.url.path + ("?" + request.url.query if request.url.query else "")
            current = request.headers.get("HX-Current-URL")
            if current:
                parsed = urlsplit(current)
                if parsed.netloc.lower() == request.headers.get("host", "").lower():
                    destination = return_path(
                        parsed.path + ("?" + parsed.query if parsed.query else "")
                    )
            url = "/signin?return_to=" + quote(destination, safe="")
            return (
                HTMLResponse("", headers={"HX-Redirect": url})
                if request.headers.get("HX-Request")
                else RedirectResponse(url, 303)
            )
        return browser_error(
            request,
            404 if error.status_code in (403, 404) else error.status_code,
            str(error.detail),
        )
    return JSONResponse({"detail": error.detail}, error.status_code, headers=error.headers)
