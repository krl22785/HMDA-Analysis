#!/bin/bash
m="$2"
n="$3"
s="$4"
t="$5"
head -n "$n" "$1" | tail -n +"$m" | cut -d, -f "$s"-"$t"
