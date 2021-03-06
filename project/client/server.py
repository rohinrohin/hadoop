import os
from client import send_request_json, start_zookeeper, get_cluster_status
from flask import Flask,render_template, request,json

app = Flask(__name__)
start_zookeeper()

@app.route('/')
def hello():
    return render_template('client_input.html')

@app.route('/request', methods=['POST'])
def requestter():
    req = request.get_json(force=True)
    response=send_request_json(req)
    print({'status':'OK', "request": req})

    #response = {
    #     "status": "SUCCESS",
    #     "data": "hello",
    #     "logger": ["Server connected to Master",
    #     "Master key not responsible",
    #     "Hello"]
    # }
    #return render_template('client_input.html', resp=response)
    return json.dumps(response);

# getting the status of the cluster
@app.route('/status', methods=['POST'])
def status_retrieval():
    return get_cluster_status()

if __name__=="__main__":
    app.run(debug=True, port=9000)
