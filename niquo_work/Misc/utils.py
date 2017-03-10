from datetime import datetime
import time
import file_constants as constants


def get_hour(timestamp):
    return get_time_obj(timestamp).hour


def get_time_obj(timestamp):
	format_string = "%Y.%m.%d %H:%M:%S"
	return datetime.strptime(timestamp,format_string)


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