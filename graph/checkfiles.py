#!/usr/bin/python
# Checks temporal consistency of the files,
# by finding the files with high time gap 
import os, sys, helper, subprocess
import datetime as dt
YEAR = 2014
pathname = sys.argv[1]
filelist = sorted(os.listdir(pathname))
filelist = [os.path.join(pathname, filename) for filename in filelist]

def readdate(line):
    """ Convert a line to a well-formed tupple """
    splitted = line.split('::') 
     
    # Convert the date
    date = dt.datetime.strptime(splitted[1].strip(), '%a %b %d %H:%M:%S')
    correctdate = date.replace(year=YEAR)
    return correctdate

def getdate(filename):
    return (readdate(subprocess.check_output(["head", "-1", filename])),
           readdate(subprocess.check_output(["tail", "-1", filename])))

_, enddate = getdate(filelist[0])
for filename in filelist[1:]:
    #print "Processing ", filename, "..."    
    startdate, enddate_next = getdate(filename)
    if (startdate - enddate).total_seconds() > 1200:
        print "Found a jump in: ", filename, (startdate - enddate) 
    enddate = enddate_next
