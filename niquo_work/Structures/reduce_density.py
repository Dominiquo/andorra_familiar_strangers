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


def main(partitioned_directory, desination_dir):
	date_files = sorted(os.listdir(partitioned_directory))[18:]
	for dfile in date_files:
		dpath = os.path.join(partitioned_directory, dfile)
		print 'loading dataframe from memory:', dfile
		df = pd.read_csv(dpath)
		df = condense_df(df)
		dest_path = os.path.join(desination_dir, dfile)
		print 'storing dataframe:', dfile
		df.to_csv(dest_path, index=False)


if __name__ == '__main__':
    main()