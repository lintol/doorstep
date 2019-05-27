import os
import requests
import time
import uuid
import json
import logging
from autobahn.wamp.exception import ApplicationError
from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import asyncio

from .wamp_server import DoorstepComponent, SessionSet
from .ini import DoorstepIni
from .metadata import DoorstepContext
from . import errors

class WampClientComponent(ApplicationSession):
    """Connector to join and execute a WAMP session."""

    _server = None
    _session = None

    def __init__(self, on_join, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_join = on_join

    async def onJoin(self, details):
        logging.error("Joined")
        return await self.on_join(self)

    async def call_server(self, endpoint, *args, **kwargs):
        """Generate the correct endpoint for the known server."""

        try:
            return await self.call(
                'com.ltldoorstep.{server}.{endpoint}'.format(server=self._server, endpoint=endpoint),
                self._session,
                *args,
                **kwargs
            )
        except Exception as e:
            if e.error.startswith('LintolDoorstep'):
                e_cls = getattr(errors, e.error)
                e = e_cls(e.kwargs['exception'], processor=e.kwargs['processor'], status_code=e.kwargs['code'], message=e.kwargs['message'])
            raise e


async def launch_wamp_real(on_join, router_url):
    runner = ApplicationRunner(url=router_url, realm='realm1')
    transport, protocol = await runner.run(lambda *args, **kwargs: WampClientComponent(
        on_join,
        *args,
        **kwargs
    ), start_loop=False)
    return runner

async def announce_wamp(on_join, router_url):
    """Announce the newly found package to the WAMP server."""

    runner = await launch_wamp(on_join, router_url)

async def launch_wamp(on_join, router_url):
    """Run the workflow against a WAMP server."""

    if router_url[0] == '#':
        router_url = router_url[1:]
        fallback = True
    else:
        fallback = False

    try:
        runner = await launch_wamp_real(on_join, router_url)
    except Exception as e:
        raise e
        if fallback:
            input("Could not find router, pausing until you start one... (press Return to continue)")
            runner = await launch_wamp_real(on_join, router_url)
    return runner
