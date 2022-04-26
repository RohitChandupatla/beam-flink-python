package edu.nwmsu.section02group05.rohit;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPageRohit implements Serializable{
    String voter;
    double rank;
    ArrayList<VotingPageRohit> voterList = new ArrayList<>();
    
    public RankedPageRohit(String voter,double rank, ArrayList<VotingPageRohit> voters){
        this.voter = voter;
        this.voterList = voters;
        this.rank = rank;
    }    
    public RankedPageRohit(String voter, ArrayList<VotingPageRohit> voters){
        this.voter = voter;
        this.voterList = voters;
        this.rank = 1.0;
    }    
    // retrun voter
    public String getVoter() {
        return voter;
    }
    // set voter
    public void setVoter(String voter) {
        this.voter = voter;
    }
    //  return voter list
    public ArrayList<VotingPageRohit> getVoterList() {
        return voterList;
    }
    // set voter list
    public void setVoterList(ArrayList<VotingPageRohit> voterList) {
        this.voterList = voterList;
    }
    // to string
    @Override
    public String toString(){
        return this.voter +"<"+ this.rank +","+ voterList +">";
    }
    // return rank
    public double getRank() {
        return this.rank;
    }
}