import logging, json
from flask import current_app, request

def main():
    current_app.logger.info(f'get data from mq:  {request.get_json(force=True)}')
    return "ok"
