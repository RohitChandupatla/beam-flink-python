# java-word-count-beam

#### This project is to find the page rank for web 04 folder

### Use the following command to generate a Maven project that contains Beam’s WordCount examples and builds against the most recent Beam release:

```
mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.36.0 `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false

```

### Command to run PR Quick Start
mvn compile exec:java -D exec.mainClass=edu.nwmsu.section02group05.shaik.MinimalPageRankShaik
