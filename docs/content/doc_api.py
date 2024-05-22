from flask import request

def main():
    FILE_HEADER = 'X-Fission-Params-File'
    try:
        file_name = request.headers[FILE_HEADER]
        with open(file_name, 'rt') as file:
            return file.read()
    except:
        return '404'
