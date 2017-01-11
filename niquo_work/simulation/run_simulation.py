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

data_path = '../../niquo_data/simulation/July_first_week_random_walk.csv'
destination_path = '../../niquo_data/simulation'
graph_path = '../../niquo_data/simulation/simulation_graph.p'


# Network.create_graph(data_path, graph_path, True)
# Network.clean_graph(graph_path, graph_path, threshold=100)

prev_main.Main(destination_path, data_path, graph_path)


sim_encounters_json = '../../niquo_data/simulation/enc'
encounters_matrix = '../../niquo_data/simulation/'
tower_stats_map = '../../niquo_data/simulation/'

ea.locations_encounters_data(sim_encounters_json, encounters_matrix)
cPickle.dump(ea.generate_stats_per_tower(sim_encounters_json), open()

create_user_pair_dict