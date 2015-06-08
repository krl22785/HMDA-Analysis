#!/bin/bash
INPUT=$1
OUTPUT=$2
HD=hadoop

${HD} fs -rm -r $OUTPUT

${HD} jar application.jar AverageMortgage $INPUT $OUTPUT