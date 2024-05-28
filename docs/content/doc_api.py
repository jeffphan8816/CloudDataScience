# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

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
