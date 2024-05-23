from flask import request
import os

def main():
    FILE_HEADER = 'X-Fission-Params-File'
    current_dir = os.path.dirname(__file__)
    try:
        file_name = request.headers[FILE_HEADER]
        with open(os.path.join(current_dir, file_name), 'rt') as file:
            return file.read()
    except:
        return '404'
