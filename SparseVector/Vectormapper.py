import sys

count =0
for line in sys.stdin:
	i=0
	if count ==0:	
		fields=line.strip().split(",")	
	else:
		field1=line.strip().split(",")	
		while i<len(fields):
			if i%2==0:	
				res=field1[i]+"\t"
			else:
				if field1[i].isalpha() or fields[i].isalpha():
					i+=1
					continue
				else:
					prod=float(field1[i])*float(fields[i])
					if prod>0:
						print res +"\t"+ str(prod)
				
			i+=1
	count+=1
