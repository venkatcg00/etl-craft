"""HTTP requests never forward credentials to a redirect target."""

from http.client import HTTPResponse
from urllib.request import HTTPRedirectHandler, Request, build_opener


class NoRedirects(HTTPRedirectHandler):
    """Refuse all redirect hops so a bearer credential never changes origin."""

    def redirect_request(
        self, req: Request, fp: object, code: int, msg: str, headers: object, newurl: str
    ) -> None:
        """Leave a redirect as a refusal for the caller to report."""
        return None


def open_request(request: Request, timeout: float) -> HTTPResponse:
    """Send one bounded request without following redirects."""
    response: HTTPResponse = build_opener(NoRedirects()).open(request, timeout=timeout)
    return response
