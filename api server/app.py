from datetime import datetime
from genericpath import exists
from flask import Flask, request, jsonify, abort
from functools import wraps
from flask_cors import CORS
import get_from_db as gt

app = Flask(__name__)
# Added CORS as this will be used locally mostly.
CORS(app)

# The actual decorator function


def require_appkey(view_function):
    @wraps(view_function)
    # the new, post-decoration function. Note *args and **kwargs here.
    def decorated_function(*args, **kwargs):
        if request.headers.get('X-Api-Key'):
            if request.headers['X-Api-Key'] == 'sensibullsucks':
                return view_function(*args, **kwargs)
            else:
                abort(401, 'Wrong API key homie!')
        else:
            abort(401, 'Stop playin with me! Get outtaa here!')
    return decorated_function


@app.route('/')
def hi_neo():
    return jsonify({'Hi Neo': 'Take the red pill!'}), 200


@app.route('/v1/fii_dii_details')
@require_appkey
def todays_data():
    data = gt.get_latest_five_days_data()
    return jsonify(data), 200


@app.route('/v1/fii_dii_details/<date>')
@require_appkey
def other_days_data(date):
    data = gt.get_days_data(date)
    return jsonify(data), 200


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
