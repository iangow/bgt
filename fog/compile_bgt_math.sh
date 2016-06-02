#!/usr/bin/env bash
cd fog
ipython nbconvert --to latex --execute bgt_math.ipynb
pdflatex >/dev/null bgt_math.tex
rm bgt_math.out bgt_math.log bgt_math.aux
open bgt_math.pdf
# cd ..
