import Structures.active_user_map as aum
import Social.Network as net
import Social.graph_encounters as ge
import Structures.InteractionMap as imap
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
	encs_paths = ['/home/niquo/niquo_data/201508-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201509-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201510-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201511-AndorraTelecom-CDR/tower_encounters/',
	'/home/niquo/niquo_data/201512-AndorraTelecom-CDR/tower_encounters/']
	for tpath in encs_paths:
		print '*****************'
		print 'about to start combining graphs for:', tpath
		Main.combine_enc_maps(tpath)
	return True

tower_enc_path = '/home/niquo/niquo_data/201507-AndorraTelecom-CDR/tower_encounters/'
root_path = '/home/niquo/niquo_data/201507-AndorraTelecom-CDR'
interaction_map = imap.InteractionMap(root_path)
interaction_map.combine_date_master_files(tower_enc_path)

combine_mult_encs_paths()