LIBPATH = /eecs/dept/course/2018-19/F/4411/mj/lib/sortmergeAssign.jar
CLASSPATH = ../:$(LIBPATH):
JAVAC     = /eecs/local/bin/javac -classpath $(CLASSPATH)
JAVA      = /eecs/local/bin/java  -classpath $(CLASSPATH)

# test Sort Merge Join
SMTest:SM_JoinTest.java
	$(JAVAC) TestDriver.java QueryCheck.java SM_JoinTest.java

sortmerge: SMTest
	$(JAVA) tests.SM_JoinTest

xx:
	jar tf $(LIBPATH)

clean:
	\rm -f *.class *~ \#* core TRACE
