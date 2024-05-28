# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

import json
import logging

from flask import request

from image import get_image_url

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

    # Expecting the prompt to be sent as a POST request
    prompt = request.get_json()

    url = get_image_url(prompt)

    return json.dumps({"Status": 200, "image_url": url})
