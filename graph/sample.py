#!/usr/bin/python
""" Converts a twitter file to time series """

import helper, sys, os, json

def main():
    if len(sys.argv) != 3:
        helper.msg("Usage: sample Path SamplingFrequency")
        exit(0)

    pathname = sys.argv[1]
    samplingfreq = int(sys.argv[2])

    filelist = sorted(os.listdir(pathname))
    filelist = [os.path.join(pathname, filename) for filename in filelist]
    (_, startdate, _) = helper.readtwitterfile(filelist[0])          
    filesize, twfile = 0, []
    for filename in filelist:    
        helper.printinline("Reading %s", filename)
        (_filesize, _, _twfile) = helper.readtwitterfile(filename)      
        filesize += _filesize
        twfile += _twfile

    timeseries = helper.totimeseries(
        twfile, startdate, samplingfreq, filesize)    
    # store metadata and time seires as a tuple
    print json.dumps((str(startdate), samplingfreq, timeseries))
    
if __name__ == "__main__":
    main()
