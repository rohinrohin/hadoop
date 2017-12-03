import os
from flask import Flask,render_template, request,json

app = Flask(__name__)

@app.route('/')
def hello():
    return render_template('client_input.html')

@app.route('/request', methods=['POST'])
def requestter():
    req = request.get_json(force=True)
    key =  req['key']
    value = req['value']
    print({'status':'OK','key':key,'val':value})
    return json.dumps({'status':'OK','key':key,'value':value});



if __name__=="__main__":
    app.run(debug=True, port=8080)