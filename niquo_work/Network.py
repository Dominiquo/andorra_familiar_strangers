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

def clean_graph(network_object_path,threshold=1000):
	friend_graph = cPickle.load(open(network_object,'rb'))
	# TODO: clean the graph
	return True


def get_distribution_encounters(encoutners_csv,network_object_path,destination_path):
	friend_graph = cPickle.load(open(network_object,'rb'))
	caller_index = -1
	receiver_index = -1

	csvout = open(destination_path,'wb')

	with open(encoutners_csv,'rb') as csvfile:
		csv_iterator = csv.reader(csvfile,delimiter=';')
		distance_csv = csv.writer(tower_file_obj,delimiter=';')
		for row in csv_iterator:
			caller = row[caller_index]
			receiver = row[receiver_index]
			# TODO
			distance = friend_graph.distance(caller,receiver)
			new_row = [caller, receiver, distance]
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