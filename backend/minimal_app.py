from flask import Flask
import os

app = Flask(__name__)

@app.before_serving
def startup():
    print("Flask app has started and is ready to receive requests!")

@app.route('/')
def hello():
    return "Hello from Minimal Flask on Railway!"

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
