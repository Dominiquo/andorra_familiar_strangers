import unittest
import crossing_paths

class TestCrossingPaths(unittest.TestCase):

	def test_find_collisions_from_tower(self):
		START_TIME_INDEX = 3
		num_hours = 2
		time_1 = (0,0,0,'2016.07.01 00:38:00')
		time_2 = (0,0,0,'2016.07.01 02:15:00')
		time_3 = (0,0,0,'2016.07.01 02:33:12')
		time_4 = (0,0,0,'2016.07.01 04:32:35')
		time_5 = (0,0,0,'2016.07.01 07:55:35')

		all_times = [time_1, time_2, time_3, time_4, time_5]
		collisions_set = {(time_1,time_2),(time_1,time_3),(time_2,time_3),(time_3,time_4)}
		collisions_eval = crossing_paths.find_collisions_from_tower(all_times,num_hours)
		self.assertEquals(collisions_eval,collisions_set)

	def test_combine_pair_mappings_one_empty(self):
		first_map = {}
		second_map = {1: {2 : ['t1','t2']}}
		new_map = crossing_paths.combine_pair_mappings(first_map,second_map)
		self.assertEquals(new_map,second_map)

	def test_combine_pair_mapping_two_simple(self):
		first_map = {1: {2: ['t1','t2'], 4: ['t8']}}
		second_map = {1: {2: ['t5','t6']}, 2: {3: ['t3','t4']}}
		combined_map = {1: {2: ['t1','t2','t5','t6'], 4: ['t8']}, 2: {3: ['t3','t4']}}
		new_map = crossing_paths.combine_pair_mappings(first_map,second_map)
		self.assertEqual(combined_map, new_map)


if __name__ == '__main__':
    unittest.main()