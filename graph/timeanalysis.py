#!/usr/bin/python
import helper, sys, string, json
from sys import stderr
from ast import literal_eval
def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

def main():

    if len(sys.argv) < 4 or len(sys.argv) > 5:
        helper.msg("Usage: timeanalysis TimeseriesFile Top ScalingFactor [plotname]\n")
        exit(0)
    filename = sys.argv[1]
    top = int(sys.argv[2])
    scalingfactor = int(sys.argv[3])    

    helper.msg("Reading the file...\n")
    filedata = json.load(open(filename))
    helper.msg("[DONE]\n")
    helper.msg("Parsing the file...\n")        
    
    startdate_str, freq, timeseries = filedata
    startdate = helper.strtodate(startdate_str)
    helper.msg("[DONE]\n")    

    perminute, sorted_perminute = {}, {}
    for category in timeseries:
        helper.msg("Changing the time scale...\n")

        perminute[category] = helper.scaletime(
            timeseries[category], scalingfactor)
        helper.msg("[DONE]\n")
        tickcount = len(perminute[category])
        
        sorted_perminute[category] = [(dict)]*tickcount

        helper.msg("Calculating the top topics...\n")
        for tick in range(tickcount):
            sorted_perminute[category][tick] = dict(
                sorted(perminute[category][tick].items(), 
                key=lambda l: l[1], reverse=True) [:top]
                )
            

    #print helper.findtops(perminute[category], top)
    print json.dumps((startdate_str, scalingfactor * freq, sorted_perminute))
    if len(sys.argv) == 5:
        plotname = sys.argv[4]
        for category in sorted_perminute:
            #print sorted_perminute    
            helper.plot(sorted_perminute[category], 
                startdate, scalingfactor * freq, plotname + " " + category)

if __name__ == "__main__":
    main()
