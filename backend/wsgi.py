"""
PythonAnywhere WSGI entry point.
In PythonAnywhere Web tab set:
  Source code:   /home/zburnside/whale_tracker/backend
  WSGI file:     /home/zburnside/whale_tracker/backend/wsgi.py
  Working dir:   /home/zburnside/whale_tracker/backend
"""
import sys
import os

sys.path.insert(0, '/home/zburnside/whale_tracker/backend')
os.environ.setdefault('MYSQL_PASSWORD', 'YOUR_PASSWORD_HERE')

from api import app
from fastapi.middleware.wsgi import WSGIMiddleware

application = WSGIMiddleware(app)
