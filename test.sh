#!/bin/bash

source ~/venv/bin/activate
python acceptor.py 1 config.txt &
python acceptor.py 2 config.txt &
python acceptor.py 3 config.txt &
python proposer.py 1 config.txt &
# python learner.py -vvv 1 config.txt&
# python client.py 2 config.txt < test_input &

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

sleep 60