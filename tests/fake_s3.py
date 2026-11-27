"""A local fake of an anonymous S3 bucket (ListObjectsV2 and GET)."""
import asyncio
import hashlib
import socket
import threading
from xml.sax.saxutils import escape

from aiohttp import web


def serve(application):
    """Run an aiohttp app in a thread; return (base_url, stop)."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    loop = asyncio.new_event_loop()
    runner = web.AppRunner(application)
    started = threading.Event()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner.setup())
        loop.run_until_complete(web.SockSite(runner, sock).start())
        started.set()
        loop.run_forever()

    threading.Thread(target=run, daemon=True).start()
    started.wait(10)

    def stop():
        loop.call_soon_threadsafe(loop.stop)

    return f"http://127.0.0.1:{port}", stop


def bucket_app(name: str, objects: dict, requests=None):
    """
    An aiohttp app serving `objects` ({key: bytes}) as bucket `name`.

    `objects` is read at each request (tests may change it); `requests`
    (a Counter) counts listings by prefix and downloads by key.
    """
    def listing(request):
        prefix = request.query.get("prefix", "")
        delimiter = request.query.get("delimiter")
        if requests is not None:
            requests[prefix] += 1
        contents, prefixes = [], set()
        for key, value in sorted(objects.items()):
            if not key.startswith(prefix):
                continue
            rest = key[len(prefix):]
            if delimiter and delimiter in rest:
                prefixes.add(prefix + rest.split(delimiter)[0] + delimiter)
                continue
            contents.append(
                f"<Contents><Key>{escape(key)}</Key>"
                f"<LastModified>2020-01-01T00:00:00.000Z</LastModified>"
                f"<ETag>&quot;{hashlib.md5(value).hexdigest()}&quot;</ETag>"
                f"<Size>{len(value)}</Size></Contents>"
            )
        common = "".join(
            f"<CommonPrefixes><Prefix>{escape(p)}</Prefix></CommonPrefixes>"
            for p in sorted(prefixes)
        )
        xml = (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/'
            '2006-03-01/">'
            f'<IsTruncated>false</IsTruncated>{"".join(contents)}{common}'
            "</ListBucketResult>"
        )
        return web.Response(text=xml, content_type="application/xml")

    async def list_objects(request):
        return listing(request)

    async def get_object(request):
        key = request.match_info["key"]
        if requests is not None:
            requests["GET " + key] += 1
        if key not in objects:
            raise web.HTTPNotFound()
        return web.Response(body=objects[key])

    application = web.Application()
    application.router.add_get(f"/{name}", list_objects)
    application.router.add_get(f"/{name}/{{key:.+}}", get_object)
    return application
