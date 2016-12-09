import networkx as nx
import RawCRDData as raw
import file_constants as constants
import cPickle

def create_graph(csv_file,store_path):
	csvData = raw.RawCDRCSV(csv_file)
	friend_graph = nx.Graph()
	caller_index = 0
	receiver_index = 16

	for row in csvData.rows_generator():
		caller = row[caller_index]
		receiver = row[receiver_index]
		friend_graph.add_edge(caller,receiver)
	cPickle.dump(friend_graph, open(store_path,'wb'))
	return True

def clean_graph(network_object_path, cleaned_path, threshold=1000):
	friend_graph = cPickle.load(open(network_object,'rb'))
	nodes = nx.nodes(friend_graph)
	degrees_dict = nx.degree(friend_graph,nodes)
	count = 0
	for node, degree in degrees_dict.iteritems():
		if degree > threshold:
			friend_graph.remove_node(node)
			count += 1
	print 'removed ', count, 'nodes from the graph'
	cPickle.dump(friend_graph,open(cleaned_path,'wb'))
	return True


def get_distribution_encounters(encoutners_csv,network_object_path,destination_path):
	# row = [caller,caller_enc,delta_days,delta_seconds,tower,next_tower,last_time]
	friend_graph = cPickle.load(open(network_object,'rb'))
	caller_index = 0
	enc_index = 1
	nodes_set = set(nx.nodes(friend_graph))
	with open(encoutners_csv,'rb') as csvfile:
		with open(destination_path,'wb') as csvout:
			csv_iterator = csv.reader(csvfile,delimiter=';')
			distance_csv = csv.writer(tower_file_obj,delimiter=';')
			for row in csv_iterator:
				caller = row[caller_index]
				encounteree = row[enc_index]
				if (caller in nodes_set) and (encounteree in nodes_set):
					distance = shortest_path_length(friend_graph, source=encounteree)
					new_row = row.append(distance)
					distance_csv.writerow(row)
	return True


def main():
	csv_file = constants.JULY_DATA_FILTERED
	save_location = '../niquo_data/filtered_data/network_object.p'
	print 'creating network graph to be saved at', save_location
	create_graph(csv_file,save_location)
	print 'graph created and stored'

if __name__ == '__main__':
    main()