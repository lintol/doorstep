import werkzeug
from flask import Flask
import os
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
api = Api(app)

processors = {}

class Processor(Resource):
    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('script', type=werkzeug.FileStorage, location='files')
        args = parse.parse_args()

        content = args['script'].read()
        filename = args['script'].filename

        module_name = os.path.splitext(os.path.basename(filename))[0]

        app.engine.add_processor(module_name, content, app.session)
        return 'Success'


class Data(Resource):
    def post(self):
        parse = reqparse.RequestParser()
        parse.add_argument('content', type=werkzeug.FileStorage, location='files')
        args = parse.parse_args()

        content = args['content'].read()
        filename = args['content'].filename

        app.engine.add_data(filename, content, app.session)
        return 'Success'


class Report(Resource):
    def get(self):
        return app.engine.execute_pipeline(app.session)

api.add_resource(Processor, '/processor')
api.add_resource(Data, '/data')
api.add_resource(Report, '/report')

def get_app():
    return app
