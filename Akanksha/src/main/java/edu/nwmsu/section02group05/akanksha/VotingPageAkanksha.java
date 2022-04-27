package edu.nwmsu.section02group05.akanksha;

import java.io.Serializable;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public  class VotingPageAkanksha extends DoFn<KV<String,Iterable<String>>,KV<String,RankedPageAkanksha>> implements Serializable{
    String voterName;
    int contributorVotes;
    double pageRank;
    public VotingPageAkanksha(String voterName,Integer contributorVotes2, double pageRank){
        this.voterName = voterName;
        this.contributorVotes = contributorVotes2;      
        this.pageRank = pageRank;  
    }

    public VotingPageAkanksha(String voterName,Integer contributorVotes2){
        this.voterName = voterName;
        this.contributorVotes = contributorVotes2;      
        this.pageRank = 1.0;  
    }
    // get voterName
    public String getVoterName() {
        return voterName;
    }
    // set voterName
    public void setVoterName(String voterName) {
        this.voterName = voterName;
    }
    // return contributorVotes
    public int getContributorVotes() {
        return contributorVotes;
    }
    // set contributorVotes
    public void setContributorVotes(int contributorVotes) {
        this.contributorVotes = contributorVotes;
    }
    // toString
    @Override
    public String toString() {
        return "voterName = "+ voterName +", Page rank = "+this.pageRank +" ContributorVotes = " + contributorVotes;
    }
    // return pageRank
    public double getPageRank() {
        return this.pageRank;
    }
    // return pageRank
    public void setPageRank(double pageRank){
        this.pageRank = pageRank;
    }


}