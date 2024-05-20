class Tests(unittest.TestCase):
    
    def test_fetch_epa(self):

        bad_keys_dict1 = {'records':[{'not_parameters':1}]}
        bad_keys_dict2 = {'records':[{'parameters':[{'not_name':1,'timeSeriesReadings':2}],
                                      'geometry':{'coordinates':3}}]}
        bad_keys_dict3 = {'records':[{'parameters':[{'name':1,'not_timeSeriesReadings':2}],
                                      'geometry':{'coordinates':3}}]}
        bad_keys_dict4 = {'records':[{'parameters':[{'name':1,'timeSeriesReadings':{'not_readings':4}}],
                                      'geometry':{'coordinates':3}}]}
        
        good_keys_dict = {'records':[{'parameters':[{'name':1,'timeSeriesReadings':{'readings':{'since':'2010-11-22T10:45:33Z',
                                                                                                'until':'2011-12-23T11:46:34Z',
                                                                                                'averageValue':5}}}],
                                      'geometry':{'coordinates':3}}]}


        self.assertEqual(fetch_epa(bad_keys_dict1), [])
        self.assertEqual(fetch_epa(bad_keys_dict2), [])
        self.assertEqual(fetch_epa(bad_keys_dict3), [])
        self.assertEqual(fetch_epa(bad_keys_dict4), [])

        self.assertEqual(fetch_epa(good_keys_dict), [{'name':1,'location':3,
                                                      'start':datetime(2010, 11, 22, 10, 45, 33),
                                                      'end':datetime(2011, 12, 23, 11, 46, 34),
                                                      'value':5}])