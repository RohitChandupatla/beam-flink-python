# beam-java by Rohit

#### This project is to determine page rank for web04 folder.

Java Quickstart for Apache Beam

<https://beam.apache.org/get-started/quickstart-java>

## Set up Environment

- Java
- Maven

## Get the Sample Project

```PowerShell
mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.36.0 `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false`
```


## Execute PR Quick Start

```PowerShell
mvn compile exec:java -D exec.mainClass=edu.nwmsu.section02group05.rohit.MinimalPageRankRohit 
```
