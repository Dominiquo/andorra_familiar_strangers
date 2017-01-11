import sys
sys.path.append("..")
import cPickle
import RawCRDData as raw
import TowersPartitioned as TP
import InteractionMap as imap
import Encounters as encs
import encounters_analysis as ea
import Network
import Main as prev_main
import update_encounters_json as up

data_path = '../../niquo_data/simulation/July_first_week_random_walk.csv'
destination_path = '../../niquo_data/simulation'
graph_path = '../../niquo_data/simulation/simulation_graph.p'


# Network.create_graph(data_path, graph_path, True)
# Network.clean_graph(graph_path, graph_path, threshold=100)

prev_main.Main(destination_path, data_path, graph_path)


sim_encounters_json = '../../niquo_data/simulation/2016.07.01_2016.07.07_encounter.json'
location_matrix_path = '../../niquo_data/simulation/location_matrix_SIM_DATA.p'
tower_stats_map = '../../niquo_data/simulation/network_tower_graph_SIM_DATA.p'
encounters_dist = '../../niquo_data/simulation/encounters_dist_map_SIM_DATA.p'
user_key_encs = '../../niquo_data/simulation/user_key_encs_SIM_DATA.p'
friends_encs_dist = '../../niquo_data/simulation/encounters_dis_map_SIM_DATA.p'

# things to be made

ea.locations_encounters_data(sim_encounters_json, location_matrix_path)

cPickle.dump(ea.generate_stats_per_tower(sim_encounters_json), open(tower_stats_map,'wb'))

user_key = up.create_user_pair_dict(sim_encounters_json)
cPickle.dump(user_key,open(user_key_encs,'wb'))

dist_vals = ea.create_box_plot(sim_encounters_json, None)
cPickle.dump(dist_vals,open(friends_encs_dist,'wb'))
