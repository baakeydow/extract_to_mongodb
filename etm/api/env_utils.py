''' env utils '''
import argparse


def parse_var(s):
    """
    Parse a key, value pair, separated by '='
    That's the reverse of ShellArgs.
    On the command line (argparse) a declaration will typically look like:
        foo=hello
    or
        foo="hello world"
    """
    items = s.split('=')
    key = items[0].strip()  # we remove blanks around keys, as is logical
    if len(items) > 1:
        # rejoin the rest:
        value = '='.join(items[1:])
    return (key, value)


def parse_vars(items):
    """
    Parse a series of key-value pairs and return a dictionary
    """
    d = {}
    if items:
        for item in items:
            key, value = parse_var(item)
            d[key] = value
    return d


class Env():

    def getEnvVar(self, var_name):
        try:
            import uwsgi  # noqa
            self.values = uwsgi.opt
        except:
            parser = argparse.ArgumentParser(description="...")
            parser.add_argument("--set",
                                metavar="KEY=VALUE",
                                nargs='+',
                                help="python3 index.py --set mode=\"runserver\" SECRET_TOKEN=\"etm-api\" PORT=\"4000\" PY_ENV=\"development\" MONGODB_URI=\"mongodb://localhost:27017\"")
            args = parser.parse_args()
            self.values = parse_vars(args.set)
        val = self.values[var_name]
        if isinstance(val, str):
            return val
        return val.decode("utf-8")


env = Env()