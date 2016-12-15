import cPickle
import networkx
import json
import csv
import itertools
import RawCRDData as raw


unfiltered_data = '../niquo_data/filtered_data/06_2017_no_data.csv'
outgoing_only = '../niquo_data/filtered_data/outgoingonly_calls_june.csv'
voicemail_map = '../niquo_data/filtered_data/voicemail_map.p'
comm_type_index = 10
caller_index = 0
receiver_index = 16
start_index = 3
end_index = 4


def filter_calls_voicemail(csv_data=unfiltered_data):
	raw_data = raw.RawCDRCSV(csv_data)
	count = 0
	with open(outgoing_only, 'wb') as outfile:
		for row in raw_data.rows_generator():
			start_time = row[start_index]
			end_time = row[end_index]
			caller = row[caller_index]
			receiver = row[receiver_index]
			comm_type = row[comm_type_index]

			if (start_time == end_time) and (comm_type == 'MOC'):
				obj = {'caller': caller, 'receiver': receiver, 'time': start_time}
				json.dump(obj, outfile)
				outfile.write('\n')
				count += 1

			elif (start_time == end_time) and (comm_type == 'MTC'):
				obj = {'caller': receiver, 'receiver': caller, 'time': start_time}
				json.dump(obj, outfile)
				outfile.write('\n')
				count += 1
		print 'wrote', count,'new lines of possible voicemail calls'

def create_voicemail_dict(json_file):
	user_hash_dict = {}
	possiblilites = [val for val in json_generator(json_file)]
	times_dict = {}
	overwritten_keys = set([])
	for val in possiblilites:
		time = val['time']
		if time in times_dict:
			times_dict[time].append(val)
		else:
			times_dict[time] = [val]

	for time,values in times_dict.iteritems():
		if len(values) < 2:
			continue
		for first, second in itertools.combinations(values, 2):
			if first['receiver'] == second['caller']:
				if (first['receiver'] in user_hash_dict) and user_hash_dict[first['receiver']] != second['receiver']:
					print 'overwriting data for key: ',first['receiver']
					overwritten_keys.add(first['receiver'])
				user_hash_dict[first['receiver']] = second['receiver']
			elif second['receiver'] == first['caller']:
				if (second['receiver'] in user_hash_dict) and user_hash_dict[second['receiver']] != first['receiver']:
					print 'overwriting data for key: ',first['receiver']
					overwritten_keys.add(second['receiver'])
				user_hash_dict[second['receiver']] = first['receiver']
	return user_hash_dict,overwritten_keys




def json_generator(json_filename):
	with open(json_filename) as infile:
		for line in infile:
			yield json.loads(line)


def main():
	filter_calls_voicemail()

if __name__ == '__main__':
    main()