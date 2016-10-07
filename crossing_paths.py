import csv
import os
import extractData as ex

START_TIME_INDEX = 3
TOWER_INDEX = 6

def partition_users_by_tower(filename,limit=float('inf')):
	data_dir = "niquo_data/"
	towers_dir = "partitioned_towers/"
	tower_file_prefix = "cdr_tower_"
	tower_path_prefix = data_dir + towers_dir
	csv_suffix = ".csv"
	current_towers = set(os.listdir(tower_path_prefix))

	with open(filename,'rb') as csvfile:
		data_csv = csv.reader(csvfile,delimiter=';')
		current_row = 0
		for row in data_csv:
			if current_row == 0:
				continue

			tower_id = row[TOWER_INDEX]
			tower_file = tower_file_prefix + str(tower_id) + csv_suffix
			tower_path = tower_path_prefix + tower_file
			if tower_file in current_towers:
				tower_file_obj = open(tower_path, 'a')
			else:
				tower_file_obj = open(tower_path,'wb')

			tower_file_csv = csv.writer(tower_file_obj,delimiter=';')
			tower_file_csv.writerow(row)
			tower_file_obj.close()

			if current_row > limit:
				break
			current_row += 1


def identify_users_that_met(file_directory):
	data_dir = "niquo_data/"
	towers_dir = "partitioned_towers/"
	tower_file_prefix = "cdr_tower_"
	tower_path_prefix = data_dir + towers_dir
	csv_suffix = ".csv"
	current_towers = set(os.listdir(tower_path_prefix))

	# TODO: check through each file of users and find pairs of users that met and when

	return True

def users_met(cdr_user_1,cdr_user_2,time_range=1):
	time_1 = cdr_user_1[START_TIME_INDEX]
	time_2 = cdr_user_2[START_TIME_INDEX]
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

def main():
	data_filename = ex.most_recent
	print "retrieving data from",data_filename
	print "partitioning data by tower name..."
	partition_users_by_tower(data_filename,limit = 10)
	print "partitioning complete"

main()
