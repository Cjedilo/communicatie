import asyncio
import logging
import os
import socket
from pathlib import Path

from aiohttp import web
import jinja2
import aiohttp_jinja2

import config
import db
import auth
import ssl_manager
from handlers.http import routes as http_routes
from handlers.ws import ws_handler
from federation import peer_ws_handler, close_session

log = logging.getLogger(__name__)


def _hostname() -> str:
    return socket.gethostname()


async def _redirect_to_https(request: web.Request) -> web.Response:
    location = request.url.with_scheme("https").with_port(config.PORT if config.PORT != 443 else None)
    raise web.HTTPMovedPermanently(location=str(location))


async def _cleanup_sessions(app: web.Application):
    """Background task: purge expired sessions every hour."""
    while True:
        await asyncio.sleep(3600)
        try:
            await db.sessions_purge_expired()
        except Exception as e:
            log.warning("Session purge failed: %s", e)


async def _start_cleanup(app: web.Application):
    app["_cleanup"] = asyncio.create_task(_cleanup_sessions(app))


async def _stop_cleanup(app: web.Application):
    app["_cleanup"].cancel()


def build_app() -> web.Application:
    app = web.Application()

    templates_dir = Path(__file__).parent / "templates"
    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(str(templates_dir)),
        autoescape=jinja2.select_autoescape(["html"]),
    )

    app.on_startup.append(db.init)
    app.on_cleanup.append(db.close)
    app.on_cleanup.append(close_session)
    app.on_startup.append(_start_cleanup)
    app.on_cleanup.append(_stop_cleanup)

    for route in http_routes:
        app.router.add_route(route[0], route[1], route[2])

    app.router.add_get("/ws",   ws_handler)
    app.router.add_get("/peer", peer_ws_handler)
    app.router.add_static("/img", config.UPLOAD_DIR, show_index=False)

    return app


def build_redirect_app() -> web.Application:
    app = web.Application()
    app.router.add_route("*", "/{path_info:.*}", _redirect_to_https)
    return app


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    Path(config.UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    ssl_ctx = ssl_manager.ensure_ssl(config.SSL_CERT, config.SSL_KEY, _hostname())

    app      = build_app()
    redirect = build_redirect_app()

    loop = asyncio.new_event_loop()

    https_runner = web.AppRunner(app)
    http_runner  = web.AppRunner(redirect)

    async def start():
        await https_runner.setup()
        await http_runner.setup()

        https_site = web.TCPSite(https_runner, config.HOST, config.PORT, ssl_context=ssl_ctx)
        http_site  = web.TCPSite(http_runner,  config.HOST, config.PORT_HTTP)

        await https_site.start()
        await http_site.start()

        log.info("HTTPS on port %d, HTTP→HTTPS redirect on port %d", config.PORT, config.PORT_HTTP)

    loop.run_until_complete(start())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(https_runner.cleanup())
        loop.run_until_complete(http_runner.cleanup())


if __name__ == "__main__":
    main()
