import csv


def users_met(time_1,time_2,time_range=1):
	year_cutoff_index = 10
	hour_start_index = 11
	hour_end_index = 13

	# TODO: refine for corner case of near midnight
	# overlaps of calls
	if time_1[:year_cutoff_index] != time_2[:year_cutoff_index]:
			return False
	
	t1_hour = int(time_1[hour_start_index:hour_end_index])
	t2_hour = int(time_2[hour_start_index:hour_end_index])

	# TODO: add minute cut-off for exactly two hour_end_index
	# range of overlap

	if abs(t1_hour - t2_hour) <= time_range:
		return True
	else:
		return False


