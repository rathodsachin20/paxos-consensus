#!/bin/bash
echo Starting paxos processes
python paxos.py list1 100 &
python paxos.py  list2 200 &
python paxos.py  list3 300 &
echo Started!!
