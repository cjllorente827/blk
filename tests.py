

def echo(*args, **kwargs):

    print(f"echo recieved arguments: {args} and keyword args: {kwargs}")
    return [*args, kwargs]