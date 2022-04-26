package edu.nwmsu.section02group05.akanksha;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPageAkanksha implements Serializable{
    String voter;
    double rank;
    ArrayList<VotingPageAkanksha> voterList = new ArrayList<>();
    
    public RankedPageAkanksha(String voter,double rank, ArrayList<VotingPageAkanksha> voters){
        this.voter = voter;
        this.voterList = voters;
        this.rank = rank;
    }    
    public RankedPageAkanksha(String voter, ArrayList<VotingPageAkanksha> voters){
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
    public ArrayList<VotingPageAkanksha> getVoterList() {
        return voterList;
    }
    // set voter list
    public void setVoterList(ArrayList<VotingPageAkanksha> voterList) {
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