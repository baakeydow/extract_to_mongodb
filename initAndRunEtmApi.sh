#!/usr/bin/env sh
processId=$(lsof -ti tcp:4000)
if [ -z "$processId" ];
then
  echo "no process found"
else
  kill -9 $processId
fi
ROOT_DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
if [ -f "$ROOT_DIR/index.py" ];
then
  python3 -m venv "$ROOT_DIR"
  pip3 install --user -r "$ROOT_DIR/requirements.txt"
  export SECRET_TOKEN="etm-to-mongodb"
  export PY_ENV="production"
  export PORT="4000"
  export MONGODB_URI="mongodb://localhost:27017/etm"
  uwsgi --ini etm.ini
else
  echo "Nope that's not quite right... you need to call this script only from the project root directory"
  echo "you launched the script from $ROOT_DIR"
fi