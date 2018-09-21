import os
from celery import Celery
import numpy as np
from celery import chord



app = Celery(__name__)
app.conf.update({
    'broker_url': os.environ['CELERY_BROKER_URL'],
    'imports': ('tasks',),
    'result_backend': os.environ['CELERY_RESULT_BACKEND'],
    'result_persistent': False,
    'task_serializer': 'json',
    'result_serializer': 'json',
    'accept_content': ['json']})

@app.task(bind=True, name='ArithAsian')
def ArithAsian(self, S0, K, T, r, sig, M_sim,N_steps=100):
    dt = T/ N_steps
    sim = [[] for m in range(0,M_sim)]
    for j in range(0,M_sim):
        S = [S0]
        for i in range(0,N_steps):
            S[i] = S[i-1]*np.exp((r-0.5*sig*sig)*dt + sig*np.sqrt(dt)*np.random.normal())
        sim[j] = (np.mean(S) -K)*np.exp(-T)
    return np.mean(sim)

@app.task(bind=True, name='mean')
def mean(self, args):
    return sum(args) / len(args)


def run_simulation():
    simulations = 100000
    per_worker = 1000
    n = int(simulations / per_worker)

    S0 = 100
    K = 120
    T = 0.5
    r = 0.01
    sig = 0.1


    chord([ArithAsian.s(
        S0=S0,
        K=K,
        T=T,
        r=r,
        sig=sig,
        n_simulation=per_worker) for i in range(0, n)], mean.s())()