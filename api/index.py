import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app import app  # noqa: F401 — re-exported for Vercel's ASGI runtime
