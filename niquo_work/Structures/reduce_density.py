import os
import pandas as pd
import numpy as np
import Misc.file_constants as constants
import Misc.utils as utils
from datetime import datetime


def condense_df(df, time_chunk=30):
	print 'getting hour column for df...'
	df = df.dropna()
	df[constants.TIME_BLOCK] = df[constants.TIMESTAMP].apply(lambda t: utils.get_time_chunk(t, time_chunk))
	print 'grouping df...'
	user_hour_group = df.groupby([constants.TOWER_NUMBER, constants.SOURCE, constants.TIME_BLOCK], as_index=False)
	print 'aggregating group into dataframe...'
	df = user_hour_group.agg({constants.DAYTIME: [np.max, np.min]})
	df.columns = [' '.join(col).strip() for col in df.columns.values]
	return df

def create_hash_function(df):
	u_vals = df[constants.SOURCE].unique()
	hash_func_dict = {}
	for i,v in enumerate(u_vals):
		hash_func_dict[v] = i
	return hash_func_dict

def create_df_dates(partitioned_directory, chunk_size=30):
	date_files = sorted(os.listdir(partitioned_directory))
	all_dfs = []
	tmp_column = 'TMP'
	for dfile in date_files:
		dpath = os.path.join(partitioned_directory, dfile)
		print 'loading dataframe from memory:', dfile
		df = pd.read_csv(dpath, nrows=100)
		df = condense_df(df, time_chunk=chunk_size)
		df[constants.DAY] = get_date_from_filename(dfile)
		all_dfs.append(df)
	combo_df = pd.concat(all_dfs)
	print 'creating hash function from values...'
	hash_func = create_hash_function(combo_df)
	combo_df = combo_df.rename(columns={constants.SOURCE: tmp_column})
	combo_df[constants.SOURCE] = combo_df[tmp_column].apply(lambda v: hash_func[v])
	del combo_df[tmp_column]
	return combo_df


def main(partitioned_directory, destination_dir, chunk_size=30):
	df = create_df_dates(partitioned_directory, chunk_size)
	combo_filepath = get_main_filename(partitioned_directory, destination_dir, chunk_size)
	print 'storing dataframe:', combo_filepath
	df.to_csv(combo_filepath, index=False)
	return True

def get_main_filename(partitioned_directory, destination_dir, chunk_size):
	date_files = sorted(os.listdir(partitioned_directory))
	all_days = [get_date_from_filename(d) for d in date_files]
	low_date = min(all_days)
	high_date = max(all_days)
	file_name = 'cdr_data_' + str(low_date) + '_' + str(high_date) + '_time_' + str(chunk_size) + '.csv'
	full_path = os.path.join(destination_dir, file_name)
	return full_path

def get_date_from_filename(filename):
	i_start = 17
	i_end = 19
	return int(filename[i_start:i_end])

if __name__ == '__main__':
    main()