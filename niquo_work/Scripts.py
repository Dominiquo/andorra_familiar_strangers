import Structures.active_user_map as aum
import Social.Network as net
import Social.graph_encounters as ge
import Structures.InteractionMap as imap
import Operations.reverse_encounters as re
import Main



# RUN month to generate encounters where months are the filename of the months in 
# '/home/workspace/yleng/filtered/'

#csv of user pairs where column 0 has users of the form user1_user2
new_friend_csv = '/home/niquo/niquo_data/sampled_pairs/100k_rand_0518.csv'
# directory where these generated files should be stored
data_dir = '/home/niquo/niquo_data/sampled_pairs/0518'
# list of months for which to generate this data
months = ['201607-AndorraTelecom-CDR']
re.create_maps_for_months(data_dir,months_paths=months, new_friend_csv=new_friend_csv)
# combines all tower encounter maps for all months and generates copies in process: only used if storage permits.
re.combine_maps_for_months(data_dir, months=months)
