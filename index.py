''' Index file for REST APIs using Flask  '''
import sys
import os
from termcolor import colored

ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
os.environ.update({'ROOT_PATH': ROOT_PATH})

PUBLIC_PATH = os.path.join(ROOT_PATH, 'public')
os.environ.update({'PUBLIC_PATH': PUBLIC_PATH})

os.environ["ROOT_PATH"] = ROOT_PATH
os.environ["PUBLIC_PATH"] = PUBLIC_PATH

sys.path.append(os.path.join(ROOT_PATH, 'etm'))

ROOT_PATH = os.environ.get('ROOT_PATH')

print(colored("index: __name__ => " + __name__, 'blue'))
print(colored("index: ROOT_PATH => " + ROOT_PATH, 'blue'))

if __name__ == 'uwsgi_file_index' or __name__ == '__main__':
    from etm import application  # noqa
