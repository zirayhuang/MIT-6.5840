#!/bin/bash

outputFile="test_output.txt"

for i in {1..100}
do
   echo "Run $i" >> "$outputFile"
   go test -run 2B >> "$outputFile"
done
