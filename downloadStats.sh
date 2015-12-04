#!/bin/bash

nowdate=$(date +%s)
curl http://192.168.100.1/cmSignalData.htm -o /home/ed/Documents/CableModemStats/modemOutput/${nowdate}.htm

exit
