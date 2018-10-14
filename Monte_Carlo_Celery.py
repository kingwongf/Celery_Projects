import os
from celery import Celery
import numpy as np
from celery import chord
import redis
import os
import logging
from flask import Flask, Response, request, jsonify
from celery import chord


app = Flask(__name__)
app.config['DEBUG'] = True
logger = logging.getLogger(__name__)


app_celery = Celery(__name__)
app_celery.conf.update({
    'broker_url': os.environ['CELERY_BROKER_URL'],
    'imports': ('tasks',),
    'result_backend': os.environ['CELERY_RESULT_BACKEND'],
    'result_persistent': False,
    'task_serializer': 'json',
    'result_serializer': 'json',
    'accept_content': ['json']})

@app_celery.task(bind=True, name='ArithAsian')
def ArithAsian(self, S0, K, T, r, sig, M_sim,N_steps=100):
    dt = T/ N_steps
    sim = [[] for m in range(0,M_sim)]
    for j in range(0,M_sim):
        S = [S0]
        for i in range(0,N_steps):
            S[i] = S[i-1]*np.exp((r-0.5*sig*sig)*dt + sig*np.sqrt(dt)*np.random.normal())
        sim[j] = (np.mean(S) -K)*np.exp(-T)
    return np.mean(sim)

@app_celery.task(bind=True, name='mean')
def mean(self, args):
    return sum(args) / len(args)



@app.route('/', methods=['POST'])
def index():
    simulations = 100000
    per_worker = 1000
    n = int(simulations / per_worker)

    S0 = 100
    K = 120
    T = 0.5
    r = 0.01
    sig = 0.1

    logger.info(f'Create chord, n={n}')

    task = chord([ArithAsian.s(
        S0=S0,
        K=K,
        T=T,
        r=r,
        sig=sig,
        n_simulation=per_worker) for i in range(0, n)], mean.s())()
    return jsonify({'id': str(task.id), 'status': task.status}), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)