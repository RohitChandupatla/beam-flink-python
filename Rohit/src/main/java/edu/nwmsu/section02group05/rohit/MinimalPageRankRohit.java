package edu.nwmsu.section02group05.rohit;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;


public class MinimalPageRankRohit {
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
   * the value into our custom RankedPageRohit Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPageRohit>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPageRohit>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPageRohit> voters = new ArrayList<VotingPageRohit>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPageRohit(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPageRohit(element.getKey(), voters)));
    }
  }

  static class Job2Mapper extends DoFn<KV<String, RankedPageRohit>, KV<String, RankedPageRohit>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPageRohit> element,
      OutputReceiver<KV<String, RankedPageRohit>> receiver) {
      int votes = 0;
      ArrayList<VotingPageRohit> voters = element.getValue().getVoterList();
      if(voters instanceof Collection){
        votes = ((Collection<VotingPageRohit>) voters).size();
      }
      for(VotingPageRohit vp: voters){
        String pageName = vp.getVoterName();
        double pageRank = vp.getPageRank();
        String contributingPageName = element.getKey();
        double contributingPageRank = element.getValue().getRank();
        VotingPageRohit contributor = new VotingPageRohit(contributingPageName,votes,contributingPageRank);
        ArrayList<VotingPageRohit> arr = new ArrayList<>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getVoterName(), new RankedPageRohit(pageName, pageRank, arr)));        
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPageRohit>>, KV<String, RankedPageRohit>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPageRohit>> element,
      OutputReceiver<KV<String, RankedPageRohit>> receiver) {
        Double dampingFactor = 0.85;
        Double updatedRank = (1 - dampingFactor);
        ArrayList<VotingPageRohit> newVoters = new ArrayList<>();
        for(RankedPageRohit rankPage:element.getValue()){
          if (rankPage != null) {
            for(VotingPageRohit votingPage:rankPage.getVoterList()){
              newVoters.add(votingPage);
              updatedRank += (dampingFactor) * votingPage.getPageRank() / (double)votingPage.getContributorVotes();
            }
          }
        }
        receiver.output(KV.of(element.getKey(),new RankedPageRohit(element.getKey(), updatedRank, newVoters)));

    }

  }

  

  public static void main(String[] args) {

   
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
   
    // Create the Pipeline object with the options we defined above
    // p= pipeline
    Pipeline p = Pipeline.create(options);
    

    String dataFolder = "web04";
    // String dataFile = "go.md";
   //  String dataPath = dataFolder + "/" + dataFile;
    //p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
    // add Pcollection pairs with mapper 1 
    PCollection<KV<String, String>> pcollectionkvpairs1 = rohitMapper1(p,"go.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs2 = rohitMapper1(p,"java.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs3 = rohitMapper1(p,"python.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkvpairs4 = rohitMapper1(p,"README.md",dataFolder);
 

    PCollectionList<KV<String, String>> pcCollectionKVpairs = 
       PCollectionList.of(pcollectionkvpairs1).and(pcollectionkvpairs2).and(pcollectionkvpairs3).and(pcollectionkvpairs4);

    PCollection<KV<String, String>> myMergedList = pcCollectionKVpairs.apply(Flatten.<KV<String,String>>pCollections());
    PCollection<KV<String, Iterable<String>>> pCollectionGroupByKey = myMergedList.apply(GroupByKey.create());
    // Convert to a custom Value object (RankedPageRohit) in preparation for Job 2
    PCollection<KV<String, RankedPageRohit>> job02Input = pCollectionGroupByKey.apply(ParDo.of(new Job1Finalizer()));
  
    PCollection<KV<String,RankedPageRohit>> job2Mapper = job02Input.apply(ParDo.of(new Job2Mapper()));
  

    PCollection<KV<String, RankedPageRohit>> job02Output = null; 
    PCollection<KV<String,Iterable<RankedPageRohit>>> job02MapperGroupbkey = job2Mapper.apply(GroupByKey.create());
    
    job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));

  
    job02MapperGroupbkey = job02Output.apply(GroupByKey.create());
    
    job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));
  
    job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));
    job02MapperGroupbkey = job02Output.apply(GroupByKey.create());    
    job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));   
    

    PCollection<String> PCollectionLinksString =  job02Output.apply(
      MapElements.into(  
        TypeDescriptors.strings()).via(
        kvtoString -> kvtoString.toString()));

       
        //
        // By default, it will write to a set of files with names like wordcounts-00001-of-00005
        // output RohitKVOutput
        PCollectionLinksString.apply(TextIO.write().to("RankedPageRohit-Rohit"));
       

        p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> rohitMapper1(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> pcolInputLines =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> pcolLines  =pcolInputLines.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcColInputEmptyLines=pcolLines.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolInputLinkLines=pcColInputEmptyLines.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> pcolInputLinks=pcolInputLinkLines.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.indexOf(")")) ));

                PCollection<KV<String, String>> pcollectionkvLinks=pcolInputLinks.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (linkline ->  KV.of(dataFile , linkline) ));
     
                   
    return pcollectionkvLinks;
  }
}