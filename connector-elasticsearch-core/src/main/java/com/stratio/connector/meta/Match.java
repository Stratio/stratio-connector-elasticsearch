/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.meta;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.LogicalStep;

import java.util.ArrayList;
import java.util.List;

public class Match extends LogicalStep{
	
	private ArrayList<String> terms;
	private int type;
	private boolean computeScore;
	private int minimunMatch=1;//0 is ignored
	private String field;
	
	public Match(Operations operations){
		super(operations);
	}
	/*public Match(String field,int type){
		terms = new ArrayList<String>();
		this.type=type;
		this.field = field;
		computeScore=true;
	}
	public Match(String field,int type, String... term){
		this(field,type, true, term);
	}
	public Match(String field,int type, boolean computeScore, String... termList){
		terms = new ArrayList<String>();
		if(termList.length !=0) {
			for(String term : termList){
				terms.add(term);
			}
		}
		this.type=type;
		this.field = field;
		this.computeScore=computeScore;
	}
	*/
	public void addTerm(String term){
		terms.add(term);
	}
	
	public void addTerms(String... termList){
		for(String term: termList){
			terms.add(term);
		}
	}
	
	public void setMinimunMatch(int minimun){
		minimunMatch=minimun;
	}
	public ArrayList<String> getTerms(){
		return terms;
	}
	public int getType(){
		return type;
	}
	public String getField(){
		return field;
	}
	public boolean computeScore(){
		return computeScore;
	}
	public int getMinimumMatch(){
		return minimunMatch;
	}
	/* (non-Javadoc)
	 * @see com.stratio.meta.common.logicalplan.LogicalStep#getPreviousSteps()
	 */
	@Override
	public List<LogicalStep> getPreviousSteps() {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.stratio.meta.common.logicalplan.LogicalStep#getFirstPrevious()
	 */
	@Override
	public LogicalStep getFirstPrevious() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
