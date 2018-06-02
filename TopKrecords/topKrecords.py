import sys

index = int(sys.argv[2])
k=int(sys.argv[1])
values = []
for line in sys.stdin:
	field = line.strip().split(",")	
	if field[index].isdigit():
		values.append(int(field[index]))
values.sort(reverse=True)
res =""
for i in range(0,k):
	res+=str(values[i])+" "
print res

	 
