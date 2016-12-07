import networkx as nx
import RawCRDData as raw
import file_constants as constants
import cPickle

def create_graph(csv_file,store_path):
	csvData = raw.RawCDRCSV(csv_file)
	friend_graph = nx.Graph()
	caller_index = 0
	receiver_index = 16

	for row in csvData.row_generator():
		caller = row[caller_index]
		receiver = row[receiver_index]
		friend_graph.add_edge(caller,receiver)
	cPickle.dump(friend_graph, open(store_path,'wb'))
	return True


def main():
	csv_file = constants.JULY_DATA_FILTERED
	save_location = '../niquo_data/filtered_data/network_object.p'
	print 'creating network graph to be saved at', save_location
	create_graph(csvfile,save_location)
	print 'graph created and stored'

if __name__ == '__main__':
    main()