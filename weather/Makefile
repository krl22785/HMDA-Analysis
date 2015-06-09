# HADOOP ENV
JAVA_SOURCES=MaxTemperature.java MaxTemperatureMapper.java MaxTemperatureReducer.java
OUTPUT_JAR=application.jar

all: $(OUTPUT_JAR)

$(OUTPUT_JAR): $(JAVA_SOURCES)
	hadoop com.sun.tools.javac.Main $(JAVA_SOURCES)
	@rm -rf $(OUTPUT_JAR)
	@jar cvf $(OUTPUT_JAR) *.class

clean:
	@rm -f *.class
	@rm -f $(OUTPUT_JAR)
