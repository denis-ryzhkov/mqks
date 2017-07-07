#!/bin/bash
HOST=$(cat /opt/mqks/server/config/local.py | grep -F "config['host']" | head -n 1 | perl -pe "s/.+'(.+)'.*/\1/")
> load_test.txt
for CLIENT in {1..2}
do client/mqks.py $HOST $CLIENT 15000 >> load_test.txt &
done
# 8 * 12500 = 100 000
# 2 * 15000 = 30 000
echo "sleep and cat load_test.txt"
sleep 2
cat load_test.txt
