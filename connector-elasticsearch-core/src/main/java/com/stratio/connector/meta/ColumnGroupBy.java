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

import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.statements.structures.selectors.GroupByFunction;
import com.stratio.meta.common.statements.structures.selectors.SelectorIdentifier;

import java.io.Serializable;
import java.util.List;


public class ColumnGroupBy{

  private String identifier;
  private GroupByFunction groupByFunction;
  private String alias;

  public ColumnGroupBy(String identifier, GroupByFunction groupByFunction, String alias) {
    this.identifier = identifier;
    this.groupByFunction = groupByFunction;
    this.alias = alias;
  }

  public String getIdentifiers() {
    return identifier;
  }
  public GroupByFunction getGroupByFunction() {
	    return groupByFunction;
	  }
  public String getAlias(){
	  return alias;
  }

}
