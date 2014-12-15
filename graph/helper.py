""" Helper methods for sampling a sorted classfied twitter file """

import os, sys, nltk
from sys import stderr
import codecs
import datetime as dt, math
import StringIO

from nltk import word_tokenize
from ast import literal_eval
import plotly.plotly as py
from plotly.graph_objs import Scatter, Data
# Constants

YEAR = 2014
DFMT = '%Y-%m-%d %H:%M:%S'
CATEGORIES, IGNORELIST = literal_eval(open('CONFIG').readline())

# We use plot.ly for plotting the data and test our algorithms
PLOTLYID, PLOTLYPASS = "your user name", "your password"
def printinline(fmt, values=()):
    """ Print a progress-like line """
    stderr.write("\033[2K\r")        
    msg(fmt, values)

def msg(fmt, values=()):
    """ Print a message to stderr. Used for diagnosis and error """        
    buf = StringIO.StringIO()
    buf.write(fmt % values)
    stderr.write(buf.getvalue())

def datetoindex(startdate, currentdate, samplingfreq):
    """ Convert a date to an index based on the frequency """
    return int(math.floor((currentdate
             - startdate).total_seconds() / samplingfreq))

def indextodate(startdate, index, samplingfreq):
    """ Convert an index to a date based on the frequency """
    return startdate + dt.timedelta(index * samplingfreq)

def strtodate(datestr):
    """ Convert a string to a date """
    return dt.datetime.strptime(datestr, DFMT)

def parseline(line):
    """ Convert a line to a well-formed tupple """
    splitted = line.split('::') 
     
    # Convert the date
    date = dt.datetime.strptime(splitted[1].strip(), '%a %b %d %H:%M:%S')
    correctdate = date.replace(year=YEAR)
    
    category = splitted[0].strip().encode('ascii', 'ignore')
    text = splitted[2].strip().lower().encode('ascii', 'ignore')
    return (correctdate, category, text)

def findnouns(text):
    """ find nouns in a text """
    # The stanford parser with gate's twitter model 
    # (From https://gate.ac.uk/wiki/twitter-postagger.html)
    # is very accurate, but the speed is very low (around 1 tweet per second)
    # from nltk.tag.stanford import POSTagger
    # twitter_postagger = POSTagger(
    #    '/home/ali/Documents/bigdata/graph/stanford-postagger-2014-08-27/models/gate-EN-twitter-fast.model', 
    #    '/home/ali/Documents/bigdata/graph/stanford-postagger-2014-08-27/stanford-postagger.jar')

    tokenized = word_tokenize(text)
    tokens = totimeseries.hunpos.tag(tokenized)
    return [token[0] for token in tokens if 
                       token[0].isalpha() and 
                       token[1].startswith('NN') and
                       not token[0] in IGNORELIST]


def totimeseries(twfile, startdate, samplingfreq, filesize):
    """ Converts a twitter file to time series """

    timeseries = {}
    timeindex = 0
    readsize = 0
    for line in twfile:

        readsize += len(line)+1
        printinline("%d bytes (%d %% of total) processed, into %d samples (%d seconds of data)", 
            (readsize, 
            math.ceil((100.0 * readsize) / filesize), 
            timeindex,
            timeindex * samplingfreq
            )
        )
        
        try:          
            datatuple = parseline(line)
        except:
            msg("Unexpected error while parsing: %s \n %s\n", 
                (line, sys.exc_info()[0])
                )           
            raise 
        (_date, _category, _text) = datatuple    

        # if first instance of the category, add it to the list
        if not _category in timeseries:
            timeseries[_category] = []

        timeindex = datetoindex(startdate, _date, samplingfreq)

        # extend the timeseries if necessary
        while len(timeseries[_category]) <= timeindex:            
            timeseries[_category] += [dict()]

        nouns = findnouns(_text)
        
        for noun in nouns:                 
            if noun in timeseries[_category][timeindex]:
                timeseries[_category][timeindex][noun] += 1
            else:
                timeseries[_category][timeindex][noun] = 1                                    
    
    return timeseries

totimeseries.hunpos = nltk.tag.HunposTagger("hunpos/english.model")

def plot(timeseries, startdate, samplingfreq, plotname):
    """ Plot a list of dictionaries, representing sampled (top) topics """
    py.sign_in(PLOTLYID, PLOTLYPASS)

    timestamps = len(timeseries)
    times = [None] * timestamps
    scatterdata = dict()

    for i in range(timestamps):
        times[i] = startdate + dt.timedelta(seconds=samplingfreq*i)    

    for timestamp in range(timestamps):
        for k in timeseries[timestamp]:
            if not k in scatterdata:
                scatterdata[k] = [0] * timestamps 
            scatterdata[k][timestamp] = timeseries[timestamp][k]

    msg("Coversion to time series finished successfully.\n")
    msg("Number of ticks: %d\n", timestamps)
    msg("Number of unique nouns: %d\n", len(scatterdata))

    data = Data([Scatter(x=times, y=scatterdata[topic], 
        name=topic) for topic in scatterdata])
    return py.plot(data, filename=plotname)

def readtwitterfile(filename):
    """ Read a twitter file and store its meta data as well as content """
    try:          
        filesize = os.path.getsize(filename)
        twfile = codecs.open(filename, encoding='utf-8')
        lines = twfile.readlines()
        (startdate, _, _) = parseline(lines[0])
        return (filesize, startdate, lines)
    except:
        msg("Unexpected error while reading: %s \n %s\n", 
            (filename, sys.exc_info()[0])
            )           
        raise 
    


def misragries(toplist, wordfreq, topranks):
    """ Tweaked Misra-Gries algorithm for high frequency estimation """    
    (word, freq) = wordfreq
    if word in toplist:
        toplist[word] += freq
    elif len(toplist) < topranks-1:
        toplist[word] = freq
    else:
        temp = dict()
        for key in toplist:
            toplist[key] = toplist[key] - freq
            if toplist[key] > 0:
                temp[key] = toplist[key]        
        toplist = temp
    return toplist


def scaletime(timeseries, scalingfactor):
    """ scaling down the time """
    scaled = []
    for timeindex in range(len(timeseries)):          
        scaledindex = timeindex / scalingfactor
        if scaledindex >= len(scaled):
            scaled += [dict()]
        for k in timeseries[timeindex]:
            if k in scaled[scaledindex]:
                scaled[scaledindex][k] += timeseries[timeindex][k]
            else:
                scaled[scaledindex][k] = timeseries[timeindex][k]
    return scaled


def findtops(timeseries, top):
    """ find #top most frequenct items in the list """
    tops = []
    for timeindex in range(len(timeseries)):  
        # Find top 
        tops += [dict()]
        for key, value in timeseries[timeindex].iteritems():
            #listsize = len(timeseries[timeindex])                
            tops[timeindex] = misragries(tops[timeindex], 
                (key, value), top)

        for key in tops[timeindex]:
            tops[timeindex][key] = timeseries[timeindex][key]
    
    return tops

