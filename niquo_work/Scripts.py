import Structures.active_user_map as aum
import Social.Network as net
import Operations.reverse_encounters as re
import Main


# re.create_maps_prev_six_months()
# aum.quick_script_generate()

# def create_net():
# 	part_path = '../niquo_data/spring_data/partitioned_data/'
# 	dest_path = '../niquo_data/small_range/friend_net_July_part_data.p'
# 	net.create_graph(part_path, dest_path)
# 	return True

# create_net()


def combine_mult_encs_paths():
	encs_paths = ['/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_5_10/',
	'/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_11_20/',
	'/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_21_50/',
	'/home/niquo/niquo_data/201507-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201508-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201509-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201510-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201511-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201512-AndorraTelecom-CDR/tower_encounters/']
	for tpath in encs_paths:
		print '*****************'
		print 'about to start combining graphs for:', tpath
		Main.combine_enc_maps(tpath)
	return True

combine_mult_encs_paths()