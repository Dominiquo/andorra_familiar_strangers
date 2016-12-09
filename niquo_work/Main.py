import os
import RawCRDData as raw
import TowersPartitioned as TP
import InteractionMap as imap
import Encounters as encs


def create_encounter(data_path, dest_path, n_vals):
	for week_path in week_paths:
			full_path = os.path.join(prefix, week_path)
			for n in range(2,20,4):
				dest_base = '%s_encounter_n_%s.csv' % (week_path, n)
				full_destination = os.path.join(dest_base,dest_base)
				encs.find_mult_enc_single_week(full_path,full_destination,n)

def Main(root_path='../niquo_data/v2_data_root', data_path='../niquo_data/filtered_data/06_2017_no_data.csv'):

	towers_dir_name = 'partitioned_towers'
	towers_path = os.path.join(root_path, towers_dir_name)
	if not os.path.exists(towers_path):
				print 'made directory', towers_path
				os.makedirs(towers_path)
	print 'partitioning raw data in tower files to be stored in directory: ', towers_path
	csvData = raw.RawCDRCSV(data_path)
	csvData.filter_and_partition(towers_path)

	paired_dir_name = 'paired_callers'
	paired_path = os.path.join(root_path,paired_dir_name)
	print 'finding user call pairs to be stored ', paired_path
	if not os.path.exists(paired_path):
				os.makedirs(paired_path)
	partitioned = TP.TowersPartitioned(towers_path)
	partitioned.pair_users_from_towers(paired_path)

	combo_dir_name = 'combined_callers'
	combo_path = os.path.join(root_path,combo_dir_name)
	if not os.path.exists(combo_path):
				os.makedirs(combo_path)
	print 'combining encounters maps to be stored at ', combo_path
	all_days_maps_dir = imap.InteractionMap.createInteractionMapsSet(paired_path)
	imap.InteractionMap.combine_interaction_maps(all_days_maps_dir,combo_path)

	n_vals = range(2,20,4)
	encounters_dir_name = 'encounters_CSVs'
	encounters_path = os.path.join(root_path,encounters_dir_name)
	if not os.path.exists(encounters_path):
				os.makedirs(encounters_path)
	create_encounter(combo_path, encounters_path, n_vals)
	return True


if __name__ == '__main__':
    Main()
