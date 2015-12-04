#!/bin/bash

currentTime=$(date +%s)
/home/ed/Documents/CableModemStats/speedtest-cli > /home/ed/Documents/CableModemStats/speeds/${currentTime}.txt

exit 0
