import pandas as pd


USE_MONTHS = ['201507-AndorraTelecom-CDR.csv',
 '201508-AndorraTelecom-CDR.csv',
 '201509-AndorraTelecom-CDR.csv',
 '201510-AndorraTelecom-CDR.csv',
 '201511-AndorraTelecom-CDR.csv',
 '201512-AndorraTelecom-CDR.csv']


def get_new_friend_set(new_friend_csv):
	friend_df = pd.read_csv(new_friend_csv)
	first_col = friend_df.columns[0]
	split_to_set = lambda tied_val: set(tied_val.split('_'))
	user_set = set([])
	for val in friend_df[first_col].values:
		user_set = user_set.union(split_to_set(val))
	return user_set
