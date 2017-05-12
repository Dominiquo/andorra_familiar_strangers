import Structures.active_user_map as aum
import Social.Network as net
import Social.graph_encounters as ge
import Structures.InteractionMap as imap
import Operations.reverse_encounters as re
import Main



# ge.get_2016_months_encs()
# net.main()

# re.create_encs_df_all()
new_friend_csv = '/home/workspace/yleng/first_call_largerThan10_flag_next3_last.csv'
data_dir = '/home/niquo/niquo_data/New_run_05072017'
# re.create_maps_for_months(data_dir='/home/niquo/niquo_data/New_run_05072017', new_friend_csv=new_friend_csv)
re.combine_maps_for_months(data_dir=data_dir)


# re.create_maps_for_months()


# aum.quick_script_generate()

# def create_net():
# 	part_path = '../niquo_data/spring_data/partitioned_data/'
# 	dest_path = '../niquo_data/small_range/friend_net_July_part_data.p'
# 	net.create_graph(part_path, dest_path)
# 	return True

# create_net()


# soc_path = '/home/niquo/niquo_data/small_range/friend_net_July_part_data_LT_100.p'
# encs_5_10 = '/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_5_10/MASTER_GRAPH.p'
# encs_11_20 = '/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_11_20/MASTER_GRAPH.p'
# encs_21_50 = '/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_21_50/MASTER_GRAPH.p'
# dest_5_10 =  '/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_5_10/results_df_LT_100.csv'
# dest_11_20 = '/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_11_20/results_df_LT_100.csv'
# dest_21_50 = '/home/niquo/niquo_data/small_range/tower_encounters_REDUCED_V2/counts_21_50/results_df_LT_100.csv'

# encs = [encs_5_10, encs_11_20, encs_21_50]
# dests = [dest_5_10, dest_11_20, dest_21_50]
# for i in range(3):
# 	ge.get_social_encounters(soc_path, encs[i], dests[i])
