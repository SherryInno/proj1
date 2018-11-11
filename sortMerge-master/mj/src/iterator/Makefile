LIBPATH = /eecs/dept/course/4411/mj/lib/sortmergeAssign.jar
CLASSPATH = ../:$(LIBPATH):
JAVAC     = /eecs/local/bin/javac -classpath $(CLASSPATH)
JAVA      = /eecs/local/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

xx:
	jar tf $(LIBPATH)

clean:
	\rm -f *.class *~ \#* core 
