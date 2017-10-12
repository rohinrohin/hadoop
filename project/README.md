# Distributed Key Value Store
Distributed Key Value project built for Big Data 2017

## Requirements
 - python 3.5
 - twisted (`pip install twisted`)
 - autobahn ('pip install autobahn')
 
## Modes
 - Master Mode (inherits from KeyStore Mode)
 - KeyStore Mode
 - Backup Mode
 
Websocket will be listening on /master, /keystore, /backup. 

Note that /backup will contain the data of the previous node in the cluster as per requirement. 

