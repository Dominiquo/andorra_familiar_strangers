import os
import cPickle
import Structures.RawCRDData as raw
import Structures.TowersPartitioned as TP

def Main(root_path = '../niquo_data/spring_data/'):

	partitioned_data_path = os.path.join(root_path, 'partitioned_data')
	tower_enc_path = os.path.join(root_path, 'tower_encounters_serial/')

	tpart = TP.TowersPartitioned(partitioned_data_path)
	# tpart.pair_users_from_towers(tower_enc_path)
	# tpart.pair_towers_multiple_days(tower_enc_path)
	tpart.pair_tower_multiple_days_serial(tower_enc_path)
	return True


if __name__ == '__main__':
    Main()
