import sys
merge=[]
k=0
for line in sys.stdin:
	list = line.split()
	i=0
	k=len(list)
	while i<len(list): 
		merge.append(list[i])
		i+=1
merge.sort(reverse=True)
for j in range(0,k):
	print merge[j]
