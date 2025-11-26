import os
import getpass
from urllib.parse import urlencode

import aiohttp
import requests


Session = requests.Session
AsyncSession = aiohttp.ClientSession
AnySession = Session | AsyncSession


def get_credentials(
    user: str | None = None,
    password: str | None = None
) -> tuple[str, str]:
    """Retrieve (or ask for) NITRC credentials"""
    user_methods = ['NITRC_USER', 'XNAT_USER', input, ValueError]
    pass_methods = ['NITRC_PASS', 'XNAT_PASS', getpass.getpass, ValueError]

    if not user:
        for method in user_methods:
            if isinstance(method, str):
                user = os.environ.get(method, None)
                if user:
                    break
            elif isinstance(method, type) and issubclass(method, Exception):
                raise ValueError(
                    'Could not guet NITRC username. '
                    'Set environment variable `NITRC_USER`'
                )
            elif callable(method):
                user = method('NITRC user: ')
                if user:
                    break

    if not password:
        for method in pass_methods:
            if isinstance(method, str):
                password = os.environ.get(method, None)
                if password:
                    break
            elif isinstance(method, type) and issubclass(method, Exception):
                raise ValueError(
                    'Could not guet NITRC password. '
                    'Set environment variable `NITRC_PASS`'
                )
            elif callable(method):
                password = getpass.getpass('NITRC password: ')
                if password:
                    break

    return user, password


class nitrc_authentifier:
    """Generate an authentification function for NITRC"""
    # Must be pickable so is a class

    def __init__(
            self,
            user: str | None = None,
            password: str | None = None
    ) -> None:
        user, password = get_credentials(user, password)
        nitrc_login = 'https://www.nitrc.org/account/login.php'
        query = urlencode(dict(form_loginname=user, form_pw=password))
        self.url = f'{nitrc_login}?{query}'

    def __call__(self, session: Session) -> Session:
        session.post(self.url, verify=False)
        return session


class nitrc_authentifier_async(nitrc_authentifier):
    """Generate an authentification function for NITRC"""
    # Must be pickable so is a class

    def __init__(
            self,
            user: str | None = None,
            password: str | None = None
    ) -> None:
        user, password = get_credentials(user, password)
        nitrc_login = 'https://www.nitrc.org/account/login.php'
        query = urlencode(dict(form_loginname=user, form_pw=password))
        self.url = f'{nitrc_login}?{query}'

    async def __call__(self, session: AnySession) -> AnySession:
        if isinstance(session, Session):
            session.post(self.url, verify=False)
        else:
            await session.post(self.url, verify_ssl=False)
        return session
