# encoding: utf-8
import json
import os
import errno


def create_dir(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def read_json(fp):
    with open(fp, 'r') as f:
        content = json.load(f)
    return content


def save_json(serializable, file_name):
    create_dir(file_name)
    
    with open(file_name, 'w') as f:
        json.dump(serializable, f)
