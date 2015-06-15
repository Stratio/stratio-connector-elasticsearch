package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.FunctionRelation;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lcisneros on 2/06/15.
 */
public abstract class ESFunction {

    private String name;

    private List<Selector> parameters;

    protected ESFunction(String name, List<Selector> paramareters) {
        this.parameters = paramareters;
        this.name = name;
    }

    public List<Selector> getParameters() {
        return parameters;
    }

    public void setParameters(List<Selector> parameters) {
        this.parameters = parameters;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public abstract QueryBuilder buildQuery();


    public static ESFunction build(FunctionRelation function) throws UnsupportedException {
        List parameters = new ArrayList();
        parameters.addAll(function.getFunctionSelectors());

        switch(function.getFunctionName()){
            case "contains":
                return new Match(parameters);
            case "match_phrase":
                return new MatchPhrase(parameters);
            case "multi_match":
                return new MultiMatch(parameters);
            default:
                throw new UnsupportedException("The function [" + function.getFunctionName() + "] is not supported");
        }
    }
}
