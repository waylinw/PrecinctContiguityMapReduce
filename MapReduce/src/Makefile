CC = gcc
CFLAGS = -Wall -Werror -g
OS = /home/wwang16/Documents/DATA_CAPSTONE/
ASGNDIR = assignment/assignment5
FILES = PrecinctContiguity
JAR = job.jar
HELPERS = minixFSHelpers.c

.PHONY : all

build : clean all

all :
	javac -cp hadoop-core-1.2.1.jar $(FILES).java
	jar -cvf $(JAR) $(FILES)*.class

clean :
	rm -f *.class *.jar

upload :
	scp $(JAR) wwang16@unix13.csc.calpoly.edu:$(OS)
