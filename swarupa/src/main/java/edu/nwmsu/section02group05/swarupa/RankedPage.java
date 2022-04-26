package edu.nwmsu.section02group05.swarupa;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPage implements Serializable{
    String voter;
    double rank;
    ArrayList<VotingPage> voterList = new ArrayList<>();
    
    public RankedPage(String voter,double rank, ArrayList<VotingPage> voters){
        this.voter = voter;
        this.voterList = voters;
        this.rank = rank;
    }    
    public RankedPage(String voter, ArrayList<VotingPage> voters){
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
    public ArrayList<VotingPage> getVoterList() {
        return voterList;
    }
    // set voter list
    public void setVoterList(ArrayList<VotingPage> voterList) {
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