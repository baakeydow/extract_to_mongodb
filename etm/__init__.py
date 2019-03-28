''' init everything  '''
import os
from etm.api.app import app

if __name__ == "etm":
    from etm.api.etl import *  # noqa
    if os.environ.get('ENV') == 'production':
        app.run(port=os.environ.get('PORT'), debug=True, host='0.0.0.0',
                use_reloader=False)  # = > for production
