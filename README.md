## Distributed Systems (CS271) Project 
## Paxos protocol implementation

This project implements Paxos protocol to achieve consensus in Distributed Systems.

In this particaular implementation, consensus is achieved on a log entry of transactions for a banking application. Five servers located at different geo-locations (using AWS instances) are used to replicate the log of transactions.


#### Usage:
```
python paxos.py <ip-file> <default-ballotno>
```

_ip-file_ : is list of ips of other servers in the network. Each line should be of format ip:port  
_default-ballotno_ (optional): is any integer used as default ballot number


#### Operations supported:

balance() - shows current balance in the account  
deposit(amount) - deposits given amount in the account (using paxos for consensus)  
withdraw(amount) - withdraws given amount from the account  
fail() - simulates failure at this server (to check fault tolerance)  
unfail() - unfail the server to function normally again
