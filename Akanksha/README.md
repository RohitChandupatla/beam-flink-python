# java-word-count-beam

<details><summary> Command to generate a Maven project that contains Beam’s WordCount examples and builds against the most recent Beam release.</summary>
  <p>
  
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
  </p>
  </details>
  
  <details><summary>Sample text</summary>
  <p>
    
  *I Created sample.txt file, Used the text of Shakespeare’s Sonnets.*
    
  </p>
  </details>
  
  
  <details><summary>To run wordcount</summary>
  
  <p>
    
 ```
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=sample.txt --output=counts" -P direct-runner
``` 
    
  </p>
  </details>
  
  <details><summary>To inspect results</summary>
  <p>
    
   ```
    $ ls counts*
   ```
    
  </p>
  </details>
  
  
  
 
  
