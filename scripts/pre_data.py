import sys
import os 

r1=sys.argv[1]
o1=sys.argv[2]
r2=sys.argv[3]
o2=sys.argv[4]

def pre(a,b):
    if os.path.exists(b):
        #os.system("unlink %s" % b)
        #os.system("ln -s  %s %s" %(a,b))
        print("Waring : rawdata links have existed")
    else:
        os.system("ln -s  %s %s" %(a,b))
    
pre(r1,o1)
pre(r2,o2)

