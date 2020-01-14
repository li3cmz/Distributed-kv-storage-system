import os
import json

def load(id, current_term, voted_for, kv):
    file_path = id + '/key.json'
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)

        current_term = data['current_term']
        voted_for = data['voted_for']
        kv = data['kv']
    else:
        save()

def save(id, current_term, voted_for, kv):
    data = {'current_term': current_term,
            'voted_for': voted_for,
            'kv': kv,
            }

    file_path = id + '/key.json'
    with open(file_path, 'w') as f:
        json.dump(data, f)