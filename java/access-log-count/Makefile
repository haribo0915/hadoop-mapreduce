
JAR = AccessLogCount.jar
CP = $(shell hadoop classpath)

.PHONY: all 

all: ${JAR}

AccessLogCount.jar: ./src/*.java
	mkdir -p ./bin
	javac -cp ${CP} -d ./bin ./src/*.java
	jar -cvf $@ -C ./bin .

