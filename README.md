# CDR Encounter: Familiar Strangers

This code is used to generate encounter networks for CDR data. We define an encounter when two users are connected to the same tower within an n minute window. The pipline for producing these networks involves several intermediate steps to reduce RAM usage. Running data full pipeline can be done more efficiently from Main. There also exists operations which is used for more specific tasks like creating graphs from a select set of users and creating random samples of user pairs.


### Structures

##### RawCRDData

This class filters the raw CDR data with a given callable function on each row and partitions this data into individual CSV files for individual days. Since the original CSV is usually too large to store in RAM, this allows us the ability to break the data up into chunks for later operations. 

```
import Misc.utils as utils
#since we want to use a filter function from utils

source_data = '/home/cdr_data.csv'
dest_dir = '/home/data_dir/partitioned_data'

raw_data = RawCDRCSV(source_data)
raw_data.filter_and_partition(dest_dir, filter_func=utils.is_comm_type_data)
```
    
This will store a new csv file for each day of the month and only include those rows that return true on the function `utils.is_comm_type_data(row)`
    
##### reduce_density
Reduce the storage density of the data from the partioned files. Since we mostly only care about a user, which tower they called to/from, and the time this was made, we analyze the months by looking at n minute window chunks and aggregating all the calls/texts/data exchanges into a max and min time of occurance within that time chunk.

For example, if a user 'abc' calls at 12:21, 12:22, 12:27, and 12:29  on tower 1, this will be represented as user='abc', min_time=12:21, max_time12:29, chunk_start=12:20, chunk_size=10. This reduces the 4 rows into a single row. The time window (chunk_size) is variable. 

```
part_dir = '/home/data_dir/partitioned_data'
dest_dir = '/home/data_dir/condensed_data'

main(part_dir, dest_dir, chunk_size=30)
```

##### TowersPartitioned
Create graph objects for each tower and for each day where there are edges where an encounter was made between two users for a given tower on a given day. This is done by looking at the time chunks from the reduce density step and creating an edge for all users within a chunk and adding edges to users in adjacent chunks if `(min(chunk+1) - max(chunk)) < encounter_window`. Thresholds for size of towers may be applied if there is too much data to feasibly run a day's data. Filters may also be applied at this step so that the produced maps are smaller than if all encouters are included. 

```
enc_window = 30
condensed_data_path = '/home/data_dir/condensed_data/month_data.csv'
destination_path = '/home/data_dir/tower_encounters'

tpart = TowersPartitioned(condensed_data_path, destination_path)
tpart.pair_users_from_towers(enc_window=enc_window)
```



##### InteractionMap
Aggregate encounter maps made for days and towers. Creates duplicate maps first combining each tower for a given day then combining the day master maps for the entire month. There is an option to delete files after they're combined, but this should be taken at your own risk. Adds the tower number to each edge and retrieves this data from the filenames, so caution should be taken if changing the generated filenames for encounter files. 

```
encounters_path = '/home/data_dir/tower_encounters'

imap = InteractionMap(encounters_path)
imap.combine_all_graphs(imap.directory)
```


##### active_user_map
Get encounter information on users that are only active a certain number of times. This is used to sample from the entire user set and filter out users that are too active and not active enough. 

**This code may need checking before use. Does not meet robustness standards of previous classes.**


##### GraphLite
A simple graph structure made using python dictionaries. The idea was that this might be more lightweight than networkx graphs since I won't be using the full newtorkx graph functionality. It is more efficient in select cases. Not good enough for full usage at the moment. 

##### timer
Small timing module when I wanted to analyze the time of finding encounters for each block. Not used reguarly in code. 


### Social

###### Network
Create social graphs for given communication data. Graphs stored as pickle files with extension '.p'. Creates directed graphs as well with functions for filtering those graphs. 

```
source_data = '/home/cdr_data.csv'
main_store_path = '/home/data_dir/social_graph.p'
filtered_store_path = '/home/data_dir/social_graph_FILTERED.p'

create_graph_directed(source_data, main_store_path)
clean_dir_graph(main_store_path, filtered_store_path, mode)
```

###### graph_encounters
create encounter csvs for each pair. Looks at both the social graph as well as the encounter graph to combine this information into a single dataframe --> csv file. 

### Misc (miscellaneous)

###### file_constants
all constants used from column names to data sources on server.

###### getMaps
creates translations from graph numbers to locations. Since there are more tower ids than towers, it aggregates those towers with the same id.

###### utils
Basic util functions from common filters to opening pickle files safely. 










