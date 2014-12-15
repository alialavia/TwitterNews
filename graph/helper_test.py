# Test suits for helpe.py methods

import helper, unittest, random

class TestSequenceFunctions(unittest.TestCase):
    def setUp(self):
        self.timeseries = eval(open("last100.ts").readline())

    def sum_wordcount(self, dictlist):
        termcount = {}
        for wordfreq in dictlist:
            for word, freq in wordfreq.iteritems():
                if word in termcount:
                    termcount[word] += freq
                else:
                    termcount[word] = freq
        return termcount

    def test_scaletime(self):
        data_in = self.timeseries  

        # Test if 0Div error raises on a call with a scalingfactor = 0
        self.assertRaises(ZeroDivisionError, 
            helper.scaletime, data_in, 0)

        # test if function changes the input\
        self.assertEqual(data_in, self.timeseries)
        
        # Test if a call with a scalingfactor = 1 maintains the series
        self.assertEqual(data_in, helper.scaletime(data_in, 1))

        # Generate a random sequence to use as scaling factors        
        randomseq = range(1, 61)
        random.shuffle(randomseq)                    
        
        # count total number of words happening in the set
        termcount_in = self.sum_wordcount(data_in)            
        
        for scalingfactor in randomseq:  
                    
            ret = helper.scaletime(data_in, scalingfactor)
            
            # Make sure input and output have the same type (list)
            self.assertEqual(type(data_in), list)
            self.assertEqual(type(ret), list)
            
            # Make sure number of ticks is equal to original size divided by SF 
            # ,or one less due to rounding error.
            self.assertTrue(
                ((len(data_in) / scalingfactor) == len(ret)) or
                ((len(data_in) / scalingfactor) == len(ret) - 1)
            )

            # Make sure total size is greater or equal to the scaleddown version            
            self.assertGreaterEqual(
                sum([len(t) for t in data_in]),
                sum([len(t) for t in ret])
            )            

            termcount_out = self.sum_wordcount(ret)            
            # checking consistency of total number of words
            self.assertEqual(
                len(termcount_out),                
                len(termcount_in)
            )                        
            # checking consistency of total number of words
            for word in termcount_in:
                self.assertEqual(
                    termcount_out[word],                
                    termcount_in[word]
                )

        



if __name__ == '__main__':
    unittest.main()
