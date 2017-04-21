from datetime import datetime
import time
import sys, os
import file_constants as constants


def get_time_chunk(timestamp, time_chunk = 30):
	time_obj = get_time_obj(timestamp)
	hour = time_obj.hour
	minute = time_obj.minute
	return round(hour + ((minute/time_chunk)*time_chunk)/float(60), 5)


def get_time_obj(timestamp):
	format_string = "%Y.%m.%d %H:%M:%S"
	try:
		time_obj = datetime.strptime(timestamp,format_string)
	except Exception as e:
		print 'improper timestamp with value:', timestamp
		print e
		return False
	return time_obj


def average_call_times(time_stamp_1,time_stamp_2):
	hour_s = 0
	hour_f = 2
	min_s = 3
	min_f = 5
	sec_s = 6
	sec_f = 8
	head = time_stamp_1[:DATE_INDEX]
	time1 = time_stamp_1[DATE_INDEX+1:]
	time2 = time_stamp_2[DATE_INDEX+1:]
	avgh = (int(time1[hour_s:hour_f]) + int(time2[hour_s:hour_f]))/2
	avgm = (int(time1[min_s:min_f]) + int(time2[min_s:min_f]))/2
	avgs = (int(time1[sec_s:sec_f]) + int(time2[sec_s:sec_f]))/2
	zero_pad = lambda v: '0' + str(v) if v < 10 else str(v)
	new_time = head + ' ' + zero_pad(avgh) + ':' + zero_pad(avgm) + ':' + str(avgs)
	return only_day_second(new_time)


def trans_date_string(timestamp):
	year_end = 4
	month_s = 5
	month_f = 7
	date_s = 8
	date_f = 10
	return timestamp[:year_end] + '_' + timestamp[month_s:month_f] + '_' + timestamp[date_s:date_f]


def trans_datetime(timestamp):
	time_object = get_time_obj(timestamp)
	return time.mktime(time_object.timetuple())


def is_comm_type_data(row):
	return row[COMM_TYPE_INDEX] != constants.COMM_DATA


def remove_foreigners(row):
 	id_val = 21303
  	CARRIER = 'ID_CDOPERADORORIGEN'
  	return row[CARRIER] == id_val


def create_dir(root_path, folder_name):
	dir_path = os.path.join(root_path, folder_name)
	if not os.path.isdir(dir_path):
		os.makedirs(dir_path)
	return dir_path