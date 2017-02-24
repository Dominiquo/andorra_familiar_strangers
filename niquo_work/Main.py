import os
import cPickle
import Structures.RawCRDData as raw
import Structures.TowersPartitioned as TP

def Main():
	data_path = '../niquo_data/spring_data/partitioned_data/'
	dest_path = '../niquo_data/spring_data/tower_encounters/'
	tpart = TP.TowersPartitioned(data_path)
	tpart.pair_users_from_towers(dest_path)
	return True


if __name__ == '__main__':
    Main()
