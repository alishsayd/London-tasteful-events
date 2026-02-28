"""WSGI entrypoint for production servers."""

from app.admin import app

__all__ = ["app"]
