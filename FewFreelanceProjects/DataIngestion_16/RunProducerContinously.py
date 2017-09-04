####################################################################################
#Author:Reddy Anil Kumar                                                           #
#Description: This script takes directory which contains the documents as argument.#
#	      It runs a jar file that parses the documents and monitors the        #
#             directory continously,if a new file is added to the directory it will#
#             pick up that file and inputs that to the jar file for parsing.       #
#@Input:  Path to the directory with documents to parse.                           #                                 
####################################################################################
#!/usr/bin/env python
import sys
import subprocess
import time


application = "scala Dathena99-assembly-1.0.jar"
count = 0
def get_drlist():
    return subprocess.check_output(["ls", folder]).decode('utf-8').strip().split("\n")
if len(sys.argv) < 2:
    print "Provide the directory path as input argument"
    sys.exit(0)
else:
    folder = sys.argv[1]
while True:
    drlist1 = get_drlist()
    time.sleep(2)
    drlist2 = get_drlist()
    if (count == 0):
	count = 1
	command = application+" '"+folder+"'"
        subprocess.Popen(["/bin/bash", "-c", command])
    else:
	    for file in [f for f in drlist2 if not f in drlist1]:
        	    command = application+" '"+folder+"/"+file+"'"
	            subprocess.Popen(["/bin/bash", "-c", command])
