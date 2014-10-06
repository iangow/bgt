#!/bin/bash
for i in {0..9}
do
  suffix=_T.xml.gz
  file_pattern=$i
  output_file=data/results$i.txt.gz
  echo "$file_pattern $output_file"
  ./parse_xml_calls.pl $file_pattern | gzip > $output_file &   
done
