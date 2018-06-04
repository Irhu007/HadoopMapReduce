import sys

for line in sys.stdin:
	line.strip()
	if line:
		(index1,index2,index3)=line.split(",")
		prod=float(index2)*float(index3)
		if(prod>0):
			print prod	
