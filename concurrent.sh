#!/bin/bash
echo Starting paxos processes
python paxos.py 2222 2223 2224 &
python paxos.py 2223 2222 2224 &
python paxos.py 2224 2223 2223 &
echo Started!!
