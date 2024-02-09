# -*- coding: utf-8 -*-
"""
This script runs the python_webapp_flask application using a development server.
"""

from os import environ
from python_webapp_flask import app
from waitress import serve
from paste.translogger import TransLogger

if __name__ == '__main__':
    serve(TransLogger(app), host='0.0.0.0', port=80)
    #app.run(host='0.0.0.0', port = 80)
    #app.run(debug = True)
