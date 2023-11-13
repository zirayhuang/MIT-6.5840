#!/bin/bash

outputFile="test_output.txt"

for i in {1..20}
do
   echo "Run $i" >> "$outputFile"
   go test -run 2 >> "$outputFile"
done
