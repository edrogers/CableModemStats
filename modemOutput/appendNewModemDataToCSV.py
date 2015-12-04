#!/usr/bin/python

import re, mmap
import glob, os

#directory of timestamp-named HTM files directly from Cable Modem
dirName="/home/ed/Documents/CableModemStats/modemOutput"
modemFiles = glob.glob(dirName+"/*.htm")

#all new data will be appended to a .csv file
csvFileName="{}/modemData.csv".format(dirName)
csvFile=open(csvFileName,'a+')

textFile=open('testOutput.txt','w+')

pHTMLBreaks=re.compile(r'<BR>')
pDataTables=re.compile(r'<TABLE align')
pElements=re.compile("[<>]")
pNumberInBracesWithSpace=re.compile("\[[0-9]\] ")
pUpstreamModulations=re.compile("\[[0-9]\] [0-9]+QAM")

#modemFiles = ["1431475081.htm","1431475381.htm","1431480361.htm"]
for modemFile in modemFiles:
    f=open(modemFile,'r+')
    try:

        lineToAppendToCSV=modemFile.replace("/home/ed/Documents/CableModemStats/modemOutput/","").replace(".htm","")+","
       
        #mmap prevents the fulltext from being pulled to memory all at once         
        fulltext=mmap.mmap(f.fileno(),0)

        fullTextNoBreaks=pHTMLBreaks.sub('',fulltext)
       
        dataTables=pDataTables.split(fullTextNoBreaks)

        #discard the preamble
        dataTables.pop(0)

        #things from the first table
        downstreamChannelIDs=[]
        downstreamFrequencies=[]
        downstreamSigToNoises=[]
        downstreamModulations=[]
        downstreamPowerLevels=[]
        #things from the second table
        upstreamValues=[]
        #things from the third table
        signalStatsChannelIDs=[]
        signalStatsUnerroreds=[]
        signalStatsCorrectables=[]
        signalStatsUncorrectables=[]

        #parse the Downstream Table
        #start by breaking the string at every "<" or ">"
        # (many of these elements will just be HTML tags)
        rawElements=pElements.split(dataTables[0])
                     
        #During outages, output can appear with two alternative
        #structures. These are handled individually: 
        if len(rawElements)==96:
            #Zero connection information / Empty Tables
            for i in range(4):
                downstreamChannelIDs.append("n/a")
                downstreamFrequencies.append("n/a")
                downstreamSigToNoises.append("n/a")
                downstreamModulations.append("n/a")
                downstreamPowerLevels.append("n/a")
        elif len(rawElements)==116:
            #Abbreviated output / Only 1 column of downstream
            for nRawElement, rawElement in enumerate(rawElements):
                # print >>textFile, "{}: {}".format(nRawElement,rawElement)
                if   nRawElement==31:
                    downstreamChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==43:
                    downstreamFrequencies.append(rawElement.replace(' Hz&nbsp;',''))
                elif nRawElement==55:
                    downstreamSigToNoises.append(rawElement.replace(' dB&nbsp;',''))
                elif nRawElement==67:
                    downstreamModulations.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==99:
                    downstreamPowerLevels.append(rawElement.replace(' dBmV\n&nbsp;',''))
            for i in range(3):
                downstreamChannelIDs.append("n/a")
                downstreamFrequencies.append("n/a")
                downstreamSigToNoises.append("n/a")
                downstreamModulations.append("n/a")
                downstreamPowerLevels.append("n/a")
        elif len(rawElements)==176:
            #Healthy output    
            for nRawElement, rawElement in enumerate(rawElements):
                # print >>textFile, "{}: {}".format(nRawElement,rawElement)
                if   nRawElement==31:
                    downstreamChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==35:
                    downstreamChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==39:
                    downstreamChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==43:
                    downstreamChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==55:
                    downstreamFrequencies.append(rawElement.replace(' Hz&nbsp;',''))
                elif nRawElement==59:
                    downstreamFrequencies.append(rawElement.replace(' Hz&nbsp;',''))
                elif nRawElement==63:
                    downstreamFrequencies.append(rawElement.replace(' Hz&nbsp;',''))
                elif nRawElement==67:
                    downstreamFrequencies.append(rawElement.replace(' Hz&nbsp;',''))
                elif nRawElement==79:
                    downstreamSigToNoises.append(rawElement.replace(' dB&nbsp;',''))
                elif nRawElement==83:
                    downstreamSigToNoises.append(rawElement.replace(' dB&nbsp;',''))
                elif nRawElement==87:
                    downstreamSigToNoises.append(rawElement.replace(' dB&nbsp;',''))
                elif nRawElement==91:
                    downstreamSigToNoises.append(rawElement.replace(' dB&nbsp;',''))
                elif nRawElement==103:
                    downstreamModulations.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==107:
                    downstreamModulations.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==111:
                    downstreamModulations.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==115:
                    downstreamModulations.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==147:
                    downstreamPowerLevels.append(rawElement.replace(' dBmV\n&nbsp;',''))
                elif nRawElement==151:
                    downstreamPowerLevels.append(rawElement.replace(' dBmV\n&nbsp;',''))
                elif nRawElement==155:
                    downstreamPowerLevels.append(rawElement.replace(' dBmV\n&nbsp;',''))
                elif nRawElement==159:
                    downstreamPowerLevels.append(rawElement.replace(' dBmV\n&nbsp;',''))

        else :
            print >>textFile, "Error: downstream : nD=={}, modemfile=={}".format(len(rawElements),modemFile)
            continue
        for downstreamChannelID in downstreamChannelIDs:   
            lineToAppendToCSV+="{},".format(downstreamChannelID)       
        for downstreamFrequencie in downstreamFrequencies:   
            lineToAppendToCSV+="{},".format(downstreamFrequencie)       
        for downstreamSigToNoise in downstreamSigToNoises:   
            lineToAppendToCSV+="{},".format(downstreamSigToNoise)       
        for downstreamModulation in downstreamModulations:   
            lineToAppendToCSV+="{},".format(downstreamModulation)       
        for downstreamPowerLevel in downstreamPowerLevels:   
            lineToAppendToCSV+="{},".format(downstreamPowerLevel)       

        #parse the Upstream Table
        #start by breaking the string at every "<" or ">"
        # (many of these elements will just be HTML tags)
        rawElements=pElements.split(dataTables[1])

        if len(rawElements) == 92:
            # Blank table
            for i in range(10):
                upstreamValues.append("n/a")
        elif len(rawElements) == 120 :
            for nRawElement, rawElement in enumerate(rawElements):
#                print >>textFile, "{}: {}".format(nRawElement,rawElement)
                if   nRawElement==31:
                    upstreamValues.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==43:
                    upstreamValues.append(rawElement.replace(' Hz&nbsp;',''))
                elif nRawElement==55:
                    upstreamValues.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==67:
                    upstreamValues.append(rawElement.replace(' Msym/sec&nbsp;',''))
                elif nRawElement==79:
                    upstreamValues.append(rawElement.replace(' dBmV&nbsp;',''))
                elif nRawElement==91:
                    upstreamModulations=pUpstreamModulations.findall(rawElement)
                    upstreamModulationsSplit=[]
                    for upstreamModulation in upstreamModulations:
                        upstreamModulationsSplit.append(re.search("\[[0-9]\]",upstreamModulation).group())
                        upstreamModulationsSplit.append(re.search("[0-9]+QAM",upstreamModulation).group())
                    while len(upstreamModulationsSplit)<4:
                        upstreamModulationsSplit.append("")
                    upstreamValues.extend(upstreamModulationsSplit)
                elif nRawElement==103:
                    upstreamValues.append(rawElement.replace('&nbsp;',''))

        else :
            print >>textFile, "Error: upstream : nD=={}, modemfile=={}".format(len(rawElements),modemFile)
            continue

        for upstreamValue in upstreamValues:
            lineToAppendToCSV+="{},".format(upstreamValue)

        #parse the Signal Stats Table
        #start by breaking the string at every "<" or ">"
        # (many of these elements will just be HTML tags)
        rawElements=pElements.split(dataTables[2])

        if len(rawElements)==72:
            for i in range(4):
                signalStatsChannelIDs.append("n/a")
                signalStatsUnerroreds.append("n/a")
                signalStatsCorrectables.append("n/a")
                signalStatsUncorrectables.append("n/a")
                
        elif len(rawElements)==88:
            for nRawElement, rawElement in enumerate(rawElements):
#                print >>textFile, "{}: {}".format(nRawElement,rawElement)
                if   nRawElement==31:
                    signalStatsChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==43:
                    signalStatsUnerroreds.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==55:
                    signalStatsCorrectables.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==67:
                    signalStatsUncorrectables.append(rawElement.replace('&nbsp;',''))
            for i in range(3):
                signalStatsChannelIDs.append("n/a")
                signalStatsUnerroreds.append("n/a")
                signalStatsCorrectables.append("n/a")
                signalStatsUncorrectables.append("n/a")

        elif len(rawElements)==136:
            for nRawElement, rawElement in enumerate(rawElements):
#                print >>textFile, "{}: {}".format(nRawElement,rawElement)
                if   nRawElement==31:
                    signalStatsChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==35:
                    signalStatsChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==39:
                    signalStatsChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==43:
                    signalStatsChannelIDs.append(rawElement.replace('&nbsp; ',''))
                elif nRawElement==55:
                    signalStatsUnerroreds.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==59:
                    signalStatsUnerroreds.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==63:
                    signalStatsUnerroreds.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==67:
                    signalStatsUnerroreds.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==79:
                    signalStatsCorrectables.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==83:
                    signalStatsCorrectables.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==87:
                    signalStatsCorrectables.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==91:
                    signalStatsCorrectables.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==103:
                    signalStatsUncorrectables.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==107:
                    signalStatsUncorrectables.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==111:
                    signalStatsUncorrectables.append(rawElement.replace('&nbsp;',''))
                elif nRawElement==115:
                    signalStatsUncorrectables.append(rawElement.replace('&nbsp;',''))

        else :
            print >>textFile, "Error: signalStats : nD=={}, modemfile=={}".format(len(rawElements),modemFile)
            continue

        for signalStatsChannelID in signalStatsChannelIDs:   
            lineToAppendToCSV+="{},".format(signalStatsChannelID)       
        for signalStatsUnerrored in signalStatsUnerroreds:   
            lineToAppendToCSV+="{},".format(signalStatsUnerrored)       
        for signalStatsCorrectable in signalStatsCorrectables:   
            lineToAppendToCSV+="{},".format(signalStatsCorrectable)       
        for signalStatsUncorrectable in signalStatsUncorrectables:   
            lineToAppendToCSV+="{},".format(signalStatsUncorrectable)       

        lineToAppendToCSV=lineToAppendToCSV.replace('\n','')
        print >>csvFile, lineToAppendToCSV.rstrip(",")
       
    finally:
        f.close()

    os.remove(modemFile)
quit()
