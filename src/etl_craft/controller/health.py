"""A standard-library probe listener, with no API or UI dependency."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from etl_craft.config.model import parse_api_address
from etl_craft.controller.supervisor import serve
from etl_craft.core.http import bind_api_address
from etl_craft.engine.metrics import install
from etl_craft.services.operations import OperationContext, to_json
from etl_craft.services.operations.metrics import scrape
from etl_craft.services.operations.processes import probe


def serve_health(ctx: OperationContext, stop: threading.Event) -> None:
    """Bind probes first, supervise in the main thread, and close the listener on exit."""
    host, port = parse_api_address(ctx.config.api_address)
    install(ctx.engine)
    listener = bind_api_address(host, port)

    class Handler(BaseHTTPRequestHandler):
        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(5)

        def do_GET(self) -> None:
            if self.path == "/metrics":
                result = scrape(ctx)
                body = result.body.encode()
                self.send_response(result.status)
                self.send_header("Content-Type", result.content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path != "/health":
                self.send_error(404)
                return
            body = json.dumps(to_json(probe(ctx))).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = HTTPServer((host, port), Handler, bind_and_activate=False)
    server.socket.close()
    server.socket = listener
    server.server_address = listener.getsockname()
    server.server_activate()
    thread = threading.Thread(target=server.serve_forever, name="etl-craft-health", daemon=True)
    thread.start()
    try:
        serve(ctx, stop)
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
