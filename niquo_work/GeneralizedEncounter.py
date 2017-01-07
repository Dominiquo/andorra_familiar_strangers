import cPickle
import getMaps as maps
import update_encounters_json as up
import pandas as pd
import os


def towerEncsDictToDataFrame(combined_caller_tower_path):
	tower_encs_dict = cPickle.load(open(combined_caller_tower_path,'rb'))
	tower_path_base = os.path.basename(combined_caller_tower_path)
	id_latlon = maps.id_to_lat_lon()
	tower_latlon = id_latlon[tower_path_base[10:-2]]
	pd_dict = {'user_1': [], 'user_2': [], 'count': [], 'tower': []}
	if tower_latlon:
		for first_user,encounters_dict in tower_encs_dict.iteritems():
			for second_user, encs_list in encounters_dict.iteritems():
				count = up.get_new_enc_count(encs_list)
				pd_dict['user_1'].append(first_user)
				pd_dict['user_2'].append(second_user)
				pd_dict['count'].append(count)
				pd_dict['tower'].append(tower_latlon)
		return pd.DataFrame(pd_dict)
	else:
		return False

def make_csv_of_combined_callers(source_path, destination_path, limit=float('inf')):
	frames = []
	count = 1
	total = len(os.listdir(source_path))
	print 'directory has ', total, 'total encounter objects'
	for encs_obj_base in os.listdir(source_path):
		print 'converting object', encs_obj_base, ':' , count, '/', total
		encs_obj_path = os.path.join(source_path, encs_obj_base)
		frames.append(towerEncsDictToDataFrame(encs_obj_path))
		count += 1
		if count > limit:
			break
	print 'concatenating all frames...'
	final_object = pd.concat(frames)
	print 'saving dataframe to csv at', destination_path
	final_object.to_csv(destination_path)
	print 'complete...'



def Main():
	source = '../niquo_data/v4_data_root/combined_callers/2016.07.01_2016.07.07/'
	dest = '../niquo_data/v4_data_root/encounters_files/all_encounters.csv'
	make_csv_of_combined_callers(source,dest,3)



if __name__ == '__main__':
    Main()

