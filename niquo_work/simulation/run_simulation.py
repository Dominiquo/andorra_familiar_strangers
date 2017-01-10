import sys
sys.path.append("..")
import cPickle
import RawCRDData as raw
import TowersPartitioned as TP
import InteractionMap as imap
import Encounters as encs
import Network
import Main as prev_main

data_path = '../../niquo_data/July_first_week_random_walk.csv'
destination_path = '../../niquo_data/simulation'
graph_path = '../../niquo_data/simulation/simulation_graph.p'


Network.create_graph(data_path, graph_path)
Network.clean_graph(graph_path, graph_path, limit=100)

prev_main.Main(destination_path, data_path, graph_path)


