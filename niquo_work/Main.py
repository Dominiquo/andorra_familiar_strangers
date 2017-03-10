import os, sys
import cPickle
import Structures.RawCRDData as raw
import Structures.TowersPartitioned as TP
import Structures.reduce_density as RD
import Misc.file_constants as constants
import Misc.utils as utils


def Main(root_path = '../niquo_data/spring_data/', all_data_path=constants.JULY_DATA_FILTERED):
	partitioned_data_path = partition_data(root_path, all_data_path)
	condense_data_path = condense_data(root_path, partitioned_data_path)
	encs_path = find_encounters(root_path, condense_data_path)
	return True


def partition_data(root_path, data_path, delimiter=';', filter_func=utils.remove_foreigners):
	destination_path = create_dir(root_path, 'partitioned_data')
	rawData = raw.RawCDRCSV(data_path)
	print 'beginning filtering on data from:', data_path
	print 'data will be stored at:', destination_path
	rawData.filter_and_partition(destination_path, delimiter=delimiter, filter_func=filter_func)
	return destination_path


def condense_data(root_path, partitioned_data_path):
	destination_path = create_dir(root_path, 'condensed_data')
	RD.main(partitioned_data_path, destination_path)
	return destination_path


def find_encounters(root_path, condensed_data_path):
	destination_path = create_dir(root_path, 'tower_encounters')
	tpart = TP.TowersPartitioned(condensed_data_path, destination_path)
	tpart.pair_users_from_towers()
	return destination_path



# ******************HELPER FUNCTIONS**************


def create_dir(root_path, folder_name):
	dir_path = os.path.join(root_path, folder_name)
	if not os.path.isdir(dir_path):
		os.makedirs(dir_path)
	return dir_path


if __name__ == '__main__':
	args = sys.argv[1:]
	if len(args) == 0:
		Main()
	elif len(args) == 2:
		root_path = args[0]
		data_path = args[1]
		Main(root_path, data_path)
	else:
		print "Incorrect args given"
