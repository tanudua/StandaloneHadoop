# StandaloneHadoop
Standalone Hadoop Platform for Windows Environment

STEPS to run the program -
a) Copy winutils folder from lib to C:// drive so the path should be C://winutils as its hardcoded in the code
b) Before running the main program, you need to pass 2 program arguments - data/input/file.txt data/output 
First argument represents input file, you can assume what is stored in HDFS in real cluster
Second argument represents output folder which Hadoop will create and store in HDFS
c) Once you run the mail program, output folder will be created with multiple files and part-r-**** are the files where data will be written
