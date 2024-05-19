from flask import Flask, request, jsonify
import json
import pickle
from elasticsearch import Elasticsearch
import numpy as np
from sklearn.linear_model import LinearRegression


ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}

RESPONSE_HEADER = 'X-Fission-Params-Response'

BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
EMPTY = json.dumps({'Status': 200, 'Data': []})


app = Flask(__name__)

# For local testing
#@app.route('/api/models', methods=['GET'])
def main():
    # Check parameters
    if RESPONSE_HEADER not in request.headers:
        return BAD_PARAMS

    # Get parameters
    try:
        with open('lin_reg_model_test.pkl', 'rb') as file:
            loaded_model = pickle.load(file)

        print(f'Loaded Model Type: {type(loaded_model)}')
        print(f'Loaded Coefficients: {loaded_model.coef_}')
        print(f'Loaded Intercept: {loaded_model.intercept_}')

        # Read the a comma-separated list in the predictors argument
        if 'predictors' in request.args:
            predictors_str = request.args.get('predictors')
            predictors = [float(value) for value in predictors_str.split(',')]

            if len(predictors) != len(loaded_model.coef_):
                return BAD_PARAMS

            predictors = np.array(predictors).reshape(1,-1)

            prediction = loaded_model.predict(predictors)[0]

            print(f'Predictors: {predictors}')
            print(f'Prediction {prediction}')

            return {'prediction': prediction}

        # Return the model as a dictionnary if no predictors have been specified
        else:
            model_dict = {'coef':list(loaded_model.coef_),
                        'intercept':loaded_model.intercept_}
            return model_dict

    except:
        return ERROR


if __name__ == '__main__':
    app.run(debug=True)


