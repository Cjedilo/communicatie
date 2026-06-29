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
    """Background task: purge expired DB sessions hourly; clean idle peer WS every 10 min."""
    from federation import cleanup_idle_sessions
    tick = 0
    while True:
        await asyncio.sleep(600)
        tick += 1
        try:
            await cleanup_idle_sessions()
        except Exception as e:
            log.warning("Idle peer session cleanup failed: %s", e)
        if tick % 6 == 0:   # every hour
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

    bp = config.BASE_PATH  # e.g. "/chat" or ""

    for route in http_routes:
        app.router.add_route(route[0], bp + route[1], route[2])

    app.router.add_get(bp + "/ws",   ws_handler)
    app.router.add_get(bp + "/peer", peer_ws_handler)
    app.router.add_static(bp + "/img", config.UPLOAD_DIR, show_index=False)

    # Redirect bare base path (no trailing slash) to base path + /
    if bp:
        async def _redirect_base(request):
            raise web.HTTPFound(bp + "/")
        app.router.add_get(bp, _redirect_base)

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

    async def start():
        await https_runner.setup()
        https_site = web.TCPSite(https_runner, config.HOST, config.PORT, ssl_context=ssl_ctx)
        await https_site.start()

        if config.PORT_HTTP:
            redirect      = build_redirect_app()
            http_runner   = web.AppRunner(redirect)
            await http_runner.setup()
            http_site = web.TCPSite(http_runner, config.HOST, config.PORT_HTTP)
            await http_site.start()
            log.info("HTTPS on port %d, HTTP→HTTPS redirect on port %d", config.PORT, config.PORT_HTTP)
        else:
            log.info("HTTPS on port %d (no HTTP redirect — running behind proxy)", config.PORT)

    loop.run_until_complete(start())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(https_runner.cleanup())


if __name__ == "__main__":
    main()
