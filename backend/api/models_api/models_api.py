import os
from flask import request
import json
import pickle
import logging
import numpy as np
from sklearn.linear_model import LinearRegression

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MODEL_NAME_HEADER = 'X-Fission-Params-ModelName'

BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
BAD_PARAMS_MODEL_NAME = json.dumps({'Status': 400, 'Message': 'There is no trained model stored with this name'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
EMPTY = json.dumps({'Status': 200, 'Data': []})


# For local testing
def main():

    logging.info('Welcome')

    # Check parameters
    if MODEL_NAME_HEADER not in request.headers:
        return BAD_PARAMS

    # Get parameters

    model_name = request.headers[MODEL_NAME_HEADER]

    # We read the corresponding model
    try:
        current_dir = os.path.dirname(__file__)

        try:
            with open(os.path.join(current_dir, f'{model_name}.pkl'), 'rb') as file:
                loaded_model = pickle.load(file)
        except FileNotFoundError:
            return BAD_PARAMS_MODEL_NAME
        
        logging.info('Pickle file loaded')
        logging.info(f'Loaded Model Type: {type(loaded_model)}')
        logging.info(f'Loaded Coefficients: {loaded_model.coef_}')
        logging.info(f'Loaded Intercept: {loaded_model.intercept_}')

        # Read the a comma-separated list in the predictors argument
        if 'predictors' in request.args:
            logging.info('Predictors detected in URL')
            predictors_str = request.args.get('predictors')
            predictors = [float(value) for value in predictors_str.split(',')]

            if len(predictors) != len(loaded_model.coef_):
                return BAD_PARAMS

            predictors = np.array(predictors).reshape(1,-1)

            prediction = loaded_model.predict(predictors)[0]

            logging.info(f'Predictors: {predictors}')
            logging.info(f'Prediction {prediction}')

            return {'prediction': prediction}

        # Return the model as a dictionnary if no predictors have been specified
        else:
            logging.info('No predictors in URL')
            model_dict = {'coef':list(loaded_model.coef_),
                          'intercept':loaded_model.intercept_}
            return model_dict

    except:
        return ERROR