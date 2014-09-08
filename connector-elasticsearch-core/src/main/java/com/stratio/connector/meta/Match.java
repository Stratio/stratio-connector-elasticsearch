/**
* Copyright (C) 2014 Stratio (http://stratio.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.stratio.connector.meta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.LogicalStep;

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
