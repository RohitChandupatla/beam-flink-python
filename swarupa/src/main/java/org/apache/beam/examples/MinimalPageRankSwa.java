/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankSwa {

  public static void main(String[] args) {

   
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
   
    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);


    String dataFolder = "web04";
    
    PCollection<KV<String, String>> pckvpair01 = swaMapper01(p,"go.md",dataFolder);
    PCollection<KV<String, String>> pckvpair02 = swaMapper01(p,"java.md",dataFolder);
    PCollection<KV<String, String>> pckvpair03 = swaMapper01(p,"python.md",dataFolder);
    PCollection<KV<String, String>> pckvpair04 = swaMapper01(p,"README.md",dataFolder);
 

    PCollectionList<KV<String, String>> pcKvpairList = PCollectionList.of(pckvpair01).and(pckvpair02).and(pckvpair03).and(pckvpair04);

    PCollection<KV<String, String>> MergedListSwa = pcKvpairList.apply(Flatten.<KV<String,String>>pCollections());

    PCollection<String> PColLinkString =  MergedListSwa.apply(
      MapElements.into(  
        TypeDescriptors.strings())
          .via((myMergeLstout) -> myMergeLstout.toString()));

       
        //
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        PColLinkString.apply(TextIO.write().to("swaKv"));
       

        p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> swaMapper01(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> pcolInputLine01 =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> pcolLine01  =pcolInputLine01.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcColInputEmptyLine01=pcolLine01.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolInputLinkLine01=pcColInputEmptyLine01.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> pcolInputLink01=pcolInputLinkLine01.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.indexOf(")")) ));

                PCollection<KV<String, String>> pcolkvLink=pcolInputLink01.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (linkline ->  KV.of(dataFile , linkline) ));
     
                   
    return pcolkvLink;
  }
}