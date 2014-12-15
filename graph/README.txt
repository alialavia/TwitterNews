1) convert a directory of classified tweets to a time series file:
    ./totimeseries.py pathtodir/ 60  > tweets.ts
where sampling frequency is 60 seconds

2) Find the top 10 topics per hour (every 60 sample) :
    ./timeanalysis.py tweets.ts 10 60
