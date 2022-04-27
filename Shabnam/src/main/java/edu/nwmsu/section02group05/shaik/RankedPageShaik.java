package edu.nwmsu.section02group05.shaik;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPageShaik implements Serializable{
    String voter;
    double rank = 1.0;
    ArrayList<VotingPageShaik> voterList = new ArrayList<>();
    
    public RankedPageShaik(String voter,double rank, ArrayList<VotingPageShaik> voters){
        this.voter = voter;
        this.voterList = voters;
        this.rank = rank;
    }    
    public RankedPageShaik(String voter, ArrayList<VotingPageShaik> voters){
        this.voter = voter;
        this.voterList = voters;
    }    
    
    public String getVoter() {
        return voter;
    }

    public void setVoter(String voter) {
        this.voter = voter;
    }

    public ArrayList<VotingPageShaik> getVoterList() {
        return voterList;
    }

    public void setVoterList(ArrayList<VotingPageShaik> voterList) {
        this.voterList = voterList;
    }

    @Override
    public String toString(){
        return this.voter +"<"+ this.rank +","+ voterList +">";
    }

    public double getRank() {
        return this.rank;
    }
}