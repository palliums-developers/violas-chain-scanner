ps -ef | grep -v grep | grep LibraExplorerCore | awk '{print $2}' | xargs kill -9
ps -ef | grep -v grep | grep ViolasExplorerCore | awk '{print $2}' | xargs kill -9
