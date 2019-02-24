#!/usr/bin/python

import re, mmap
import glob, os

#directory of timestamp-named HTM files directly from Cable Modem
dirName="/home/ed/Documents/CableModemStats/speeds"
modemFiles = glob.glob(dirName+"/*.txt")
modemFiles.sort(key=os.path.getmtime)

#all new data will be appended to a .csv file
csvFileName="{}/speeds.csv".format(dirName)
csvFile=open(csvFileName,'a+')

# failureFileName='failedParse.log'
# failureFile=open(failureFileName,'a+')

reDecimalNumberFollowedByMs="[0-9]{1,}\.[0-9]{1,} ms"
reDecimalNumberFollowedByMbitsPerS="[0-9]{1,}\.[0-9]{1,} Mbits/s"
reLinesStartingWithHosted="Hosted by .*"
reWordsNumsCommasAndDots="[0-9A-Za-z ,\.-]*"

headerLine="Time,Ping (ms),Download Speed (Mbits/s),Upload Speed (Mbits/s),Host,Host Location,Host Distance (km)"
# print >> csvFile, headerLine

for modemFile in modemFiles:

    f=open(modemFile,'r+')
    try:

        lineToAppendToCSV=modemFile.replace("/home/ed/Documents/CableModemStats/speeds/","").replace(".txt","")+","
       
        fulltext=f.read()

        ping = re.search(reDecimalNumberFollowedByMs,fulltext)
        speeds = re.findall(reDecimalNumberFollowedByMbitsPerS,fulltext)
        hostedLine = re.search(reLinesStartingWithHosted,fulltext)

        hostName=None
        hostLoc=None
        hostDist=None
        if (hostedLine != None):
            hostList = re.findall(reWordsNumsCommasAndDots,hostedLine.group())
            hostName = hostList[0].replace("Hosted by","").strip().replace(",",";")
            hostLoc  = hostList[2].replace(",",";")
            hostDist = hostList[6].replace(" km","")
        else :
            hostName = "N/A"
            hostLoc  = "N/A"
            hostDist = "N/A"

        if ping == None:
            ping = "N/A"
        else :
            ping = ping.group().replace(" ms","")

        if len(speeds) != 2:
            speeds=["N/A","N/A"]

        lineToAppendToCSV+="{},".format(ping)
        lineToAppendToCSV+="{},".format(speeds[0].replace(" Mbits/s",""))
        lineToAppendToCSV+="{},".format(speeds[1].replace(" Mbits/s",""))
        lineToAppendToCSV+="{},".format(hostName)
        lineToAppendToCSV+="{},".format(hostLoc)
        lineToAppendToCSV+="{},".format(hostDist)
    
        print >>csvFile, lineToAppendToCSV.rstrip(",")
                    
    finally:
        f.close()

    os.remove(modemFile)
quit()
