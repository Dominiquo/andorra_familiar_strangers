#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, csv, json, itertools
import pandas as pd

def create_voicemail_dict(raw_data, writing = False):
    output_file = 'filtered' + raw_data.split('/')[-1]
    mapping_file = 'mapping' + raw_data.split('/')[-1] + '.json'
    os.system('cut -d";" -f1,4,5,6,7,11,15,17 {} > {}'.format(raw_data, output_file))
    data_raw = pd.read_csv(output_file, sep = ';')
    user_hash_dict = {}
    times_dict = {}
    overwritten_keys = set([])
    data = data_raw[data_raw['ID_CDTIPUSCOM'].isin(['MTC', 'MOC'])]
    data = data.sort(['DT_CDDATAINICI', 'DT_CDDATAFI'], ascending = [1, 1])
    inic = data[u'DT_CDDATAINICI'].values
    fi = data[u'DT_CDDATAFI'].values

    caller = data[u'DS_CDNUMORIGEN'].values
    receiver = data[u'DS_CDNUMDESTI'].values
    commu_type = data[u'ID_CDTIPUSCOM'].values
    duration = data[u'NUM_DURADA'].values

    for i in range(len(data)):
        time = (inic[i], fi[i])
        if time in times_dict:
            times_dict[time].append([caller[i], receiver[i], commu_type[i]])
        else:
            times_dict[time] = [[caller[i], receiver[i], commu_type[i]]]

    for time,values in times_dict.iteritems():
        if len(values) < 2:
            continue
        for first, second in itertools.combinations(values, 2):
            #check if first caller is the same as the second receiver or vice versa
            if first[1] == second[1]:
                print first, 'and', second
                if first[2] == 'MTC' and second[2] == 'MOC':
                    user_hash_dict[first[1]] = first[0]
                elif first[2] == 'MOC' and second[2] == 'MTC':
                    user_hash_dict[second[1]] = second[1]

    for rep_key in overwritten_keys:
        user_hash_dict.pop(rep_key,None)
    
    del times_dict
    
    if writing:
        with open(mapping_file, 'w') as outfile:
            json.dump(dict((v,k) for k,v in user_hash_dict.iteritems()), outfile)
    
    data_raw['DT_CDDATAINICI'] = [user_hash_dict(i) if i in user_hash_dict else i for i in data_raw['DT_CDDATAINICI'].values]
    data_raw['DT_CDDATAFI'] = [user_hash_dict(i) if i in user_hash_dict else i for i in data_raw['DT_CDDATAFI'].values]


    keep_list = []
    inic = data_raw[u'DT_CDDATAINICI'].values
    fi = data_raw[u'DT_CDDATAFI'].values 
    for i in range(len(inic)):
        if inic[i] != fi[i]:
            keep_list.append(i)
    data_raw = data_raw[data_raw.index.isin(keep_list)]
    data_raw.to_csv(output_file, index = False)



def cut_witness_file(file_name):
    data = pd.read_csv(file_name, sep = ',')
    data['day'] = [(int(i[8:10]) * 24 + int(i[11:13]) - 21)/24 for i in data['DT_CDDATAINICI'].values]
    data = data[data['ID_CDTIPUSCOM'].isin(['MTC', 'MOC'])]
    data = data.reset_index()
    start = data['DT_CDDATAINICI'].values
    end = data['DT_CDDATAFI'].values
    success_call = [i for i in range(len(start)) if start[i] != end[i]]
        #data = data[data.index.isin(success_call)]
    for name, group in data.groupby('day'):
        group.sort('DT_CDDATAINICI', ascending = [1])
        group.to_csv('./raw_cdr_new/raw_cdr_{}.csv'.format(name), index = False)


if __name__ == '__main__':
    
    #raw_data = '/home/data_repository/datasets/telecom/cdr/201607-AndorraTelecom-CDR.csv'
#    raw_data = '/home/data_repository/datasets/telecom/cdr/201606-AndorraTelecom-CDR.csv'
    raw_data = '/home/data_repository/datasets/telecom/cdr/201605-AndorraTelecom-CDR.csv'
    create_voicemail_dict(raw_data, writing = False)
 #   cut_witness_file('filtered' + raw_data.split('/')[-1])
