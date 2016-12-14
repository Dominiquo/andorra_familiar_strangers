import cPickle
import networkx
import json
import csv
import RawCDRCSV as raw


unfiltered_data = '../niquo_data/filtered_data/06_2017_no_data.csv'
outgoing_only = '../niquo_data/filtered_data/outgoingonly_calls_june.csv'
voicemail_map = '../niquo_data/filtered_data/voicemail_map.p'
comm_type_index = 10
caller_index = 0
receiver_index = 16
start_index = 3
end_index = 4


def filter_calls_voicemail():
	raw_data = raw.RawCDRCSV(unfiltered_data)
	count = 0
	with open(outgoing_only, 'wb') as outfile:
		for row in raw_data.row_generator():
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


def main():
	filter_calls_voicemail()

if __name__ == '__main__':
    main()