
 package edu.nwmsu.section02group05.shaik;



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

public class MinimalPageRankShaik {
   // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }

  public static void main(String[] args) {

   
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
   
    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);


    String dataFolder = "web04";
   
    PCollection<KV<String, String>> keyvalpair01 = Mapper1(p,"go.md",dataFolder);
    PCollection<KV<String, String>> keyvalpair02 = Mapper1(p,"java.md",dataFolder);
    PCollection<KV<String, String>> keyvalpair03 = Mapper1(p,"python.md",dataFolder);
    PCollection<KV<String, String>> keyvalpair04 = Mapper1(p,"README.md",dataFolder);
 

    PCollectionList<KV<String, String>> keyvalpairList = PCollectionList.of(keyvalpair01).and(keyvalpair02).and(keyvalpair03).and(keyvalpair04);

    PCollection<KV<String, String>> MergedListShaik = keyvalpairList.apply(Flatten.<KV<String,String>>pCollections());

    PCollection<String> PCLinkString =  MergedListShaik.apply(
      MapElements.into(  
        TypeDescriptors.strings())
          .via((myMergeLstout) -> myMergeLstout.toString()));

       
        //
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        PCLinkString.apply(TextIO.write().to("PageRank-Shaik"));
       
        p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> Mapper1(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> pCLInputLine01 =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> pCLine01  =pCLInputLine01.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pCInputEmptyLine01=pCLine01.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pCInputLinkLine01=pCInputEmptyLine01.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> pCInputLink01=pCInputLinkLine01.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.indexOf(")")) ));

                PCollection<KV<String, String>> shaoutput=pCInputLink01.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (linkline ->  KV.of(dataFile , linkline) ));
     
                   
    return shaoutput;
  }
}