from timer import Timer

def some_loop(cyc, timer):
	with timer as t:
		c = 0
		for i in range(cyc):
			c += 1
		return c


t = Timer('dopeasstimer',1,verbose=True)