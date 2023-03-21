from flask import Flask
from flask import jsonify
from flask import request
from home import home_bp
from contact import contact_bp
from key_creation.Api_Key_Management import ApiKeyManagement

app = Flask(__name__)


# @app.before_request
# def before():
#     print("This is executed BEFORE each request.")


@app.route('/api/<string:section>')
def api_route(section):
    """
    Route for api calls.
    Query params are expressed via 'api-key'
    """
    key = request.args.get('api-key')
    is_valid_key = ApiKeyManagement().is_valid_api_key(key)
    if is_valid_key:
        return key + section
    return key + 'is not valid'


@app.route('/create-api-key/')
def create_api_key():
    """
    Method to create a new api key if email is unique
    Query param is expressed via 'email'
    """
    email = request.args.get('email')
    create_key_rtn = ApiKeyManagement().get_api_key(email)
    return create_key_rtn


@app.route('/<string:add_name>/')
def hello(add_name):
    return "Hello " + add_name


@app.route('/<int:number>/')
def incrementor(number):
    return str(number) + " squared is " + str(number * number)


@app.route('/jason/')
def helloJason():
    return jsonify({'name': 'Jason', 'address': '123 Anywhere Ave.'})


@app.route('/serialize/')
def serializeNumbers():
    return jsonify(list(range(5))), 418

# Blueprints allow us to separate various endpoints into subdomains


app.register_blueprint(home_bp, url_prefix='/home')  # need to call /home/hello
app.register_blueprint(contact_bp, url_prefix='/contact')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105, use_reloader=True)
