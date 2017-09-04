#!/bin/bash
# apt install graphviz
cd dot
for DOT in *.dot
do
    PNG=${DOT/.dot/.png}
    echo "$DOT -> $PNG"
    dot -Tpng -o ../png/$PNG $DOT
done
