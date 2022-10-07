import inspect, hashlib

def createHashCode(self):

    # get the source code of the operation
    source = inspect.getsource(self.operation)

    def remove_all_whitespace(str):
        ws_chars = [' ', '\t', '\n']
        for char in ws_chars:
            str = str.replace(char, '')
        return str

    def append_args(target, args):
        for a in args.values():
            target += str(a)
        return target

    # scrap the whitespace to prevent unnecessary 
    # re-queries
    source_no_ws = remove_all_whitespace(source)

    # concatenate it with the arguments
    target = append_args(source_no_ws, self.arguments)
    
    # convert string to a hash
    hash = hashlib.md5(target.encode()).hexdigest()
    self.hashcode = hash
    self.output_file = hash
    return hash