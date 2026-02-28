"""WSGI entrypoint for production servers."""

from seed_orgs.admin import app

__all__ = ["app"]
