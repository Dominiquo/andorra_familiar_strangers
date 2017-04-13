import cPickle
import json

class GraphLite(object):
	def __init__(self, base_dict={}):
		self.base_dict = base_dict
	
	def add_edge(self, first, second, attr_dict):
		if first == second:
			return False

		source = max(first, second)
		dest = min(first, second)
		if source not in self.base_dict:
			self.base_dict[source] = {}
			self.combine_attr_dict(source, dest, attr_dict, first_dict=True)
		else:
			if dest in self.base_dict[source]:
				self.combine_attr_dict(source, dest, attr_dict)
			else:
				self.combine_attr_dict(source, dest, attr_dict, first_dict=True)
		return True


	def combine_attr_dict(self, source, dest, new_dict, first_dict=False):
		if first_dict:
			self.base_dict[source][dest] = {k:[v] for k,v in new_dict.iteritems()}
		else:
			current_dict = self.base_dict[source][dest]
			for k,v in new_dict.iteritems():
				current_dict[k].append(v)


	def store_object(self, dest_path):
		with open(dest_path, 'wb') as outfile:
			print 'storing JSON in:', dest_path
			json.dump(self.base_dict, outfile)

def load_object(path):
	with open(path, 'rb') as infile:
		return GraphLite(base_dict=json.load(infile))