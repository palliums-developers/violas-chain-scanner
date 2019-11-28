ps -ef | grep -v grep | grep explorer-core | awk '{print $2}' | xargs kill -9
