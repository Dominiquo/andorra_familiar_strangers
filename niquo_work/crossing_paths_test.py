import unittest
import crossing_paths

class TestCrossingPaths(unittest.TestCase):

	def test_find_collisions_from_tower(self):
		START_TIME_INDEX = 3
		time_1 = (0,0,0,'2016.07.01 00:38:00')
		time_2 = (0,0,0,'2016.07.01 02:15:00')
		time_3 = (0,0,0,'2016.07.01 02:33:12')
		time_4 = (0,0,0,'2016.07.01 04:32:35')
		time_5 = (0,0,0,'2016.07.01 07:55:35')

		all_times = [time_1, time_2, time_3, time_4, time_5]
		collisions_set = {(time_1,time_2),(time_1,time_3),(time_2,time_3),(time_3,time_4)}
		collisions_eval = crossing_paths.find_collisions_from_tower(all_times)
		self.assertEquals(collisions_eval,collisions_set)


if __name__ == '__main__':
    unittest.main()