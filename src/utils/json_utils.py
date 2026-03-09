import numpy as np

def make_json_serializable(obj):
    """
    Recursively convert numpy / pandas objects to native Python types
    """
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [make_json_serializable(i) for i in obj]

    elif isinstance(obj, tuple):
        return tuple(make_json_serializable(i) for i in obj)

    elif isinstance(obj, np.generic):
        return obj.item()

    else:
        return obj
