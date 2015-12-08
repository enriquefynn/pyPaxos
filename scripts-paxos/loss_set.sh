#!/bin/bash

LOSS=0.05

sudo iptables -A INPUT -d 239.0.0.1\
    -m statistic --mode random --probability $LOSS -j DROP
