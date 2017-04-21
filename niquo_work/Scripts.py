import Structures.active_user_map as aum
import Social.Network as net
import Operations.reverse_encounters as re
import Main


Main.combine_enc_maps('/home/niquo/niquo_data/201507-AndorraTelecom-CDR/tower_encounters/')

# re.create_maps_prev_six_months()
# aum.quick_script_generate()

# def create_net():
# 	part_path = '../niquo_data/spring_data/partitioned_data/'
# 	dest_path = '../niquo_data/small_range/friend_net_July_part_data.p'
# 	net.create_graph(part_path, dest_path)
# 	return True

# create_net()