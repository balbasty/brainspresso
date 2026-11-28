"""Constants of the downloaders (chunk size, timeouts, retries)."""
import aiohttp

CHUNK_SIZE: int = 1024 * 1024 * 8

# aiohttp's default session timeout is `ClientTimeout(total=300)`, which
# applies to the *whole* response, body included: any file that takes
# more than 5 minutes to download is killed. We only want to bail out when
# the connection cannot be established or when it stalls.
TIMEOUT = aiohttp.ClientTimeout(
    total=None,         # no limit on the total duration of a download
    sock_connect=30,    # time to establish a connection
    sock_read=120,      # maximum time without receiving any byte
)

# Status codes that are worth retrying
RETRY_STATUSES: frozenset[int] = frozenset({
    400,    # Bad Request - https://github.com/dandi/dandi-cli/issues/87
    408,    # Request Timeout
    425,    # Too Early
    429,    # Too Many Requests
    500,    # Internal Server Error
    502,    # Bad Gateway
    503,    # Service Unavailable
    504,    # Gateway Timeout
})
