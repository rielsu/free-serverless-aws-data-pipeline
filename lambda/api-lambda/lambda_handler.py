import awsgi
from flask_app import app

def handler(event, context):
    return awsgi.response(app, event, context)
