package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Map;

/**
 * Class that includes all methods associated with query functions included in select statement
 *
 * Created by alejandro on 7/07/15.
 */
public class SelectorUtils {

    // Function names
    private static final String SUB_FIELD_FUNCTION = "sub_field";


    /**
     * Retrieves the field name associated to a given selector (It may be both a basic field or a function. Functions must have their own process in order to get the
     * field name. If no process is defined the function name is returned as selector field name
     *
     * @param selector  object defining a field or a function
     * @return                  name associated to the selector
     */
    public static String getSelectorFieldName (Selector selector){
        // If no selector is provided null is returned
        if (null == selector){return null;}
        // If the selector is a function it is processed in order to get the field name
        if (selector instanceof FunctionSelector){
            return getFunctionSelectorFieldName((FunctionSelector) selector);
        }
        // Otherwise the selector is a field, so its name is returned
        return selector.getColumnName().getName();
    }

    /**
     * Method that retrieves the field name associated to the given function. Every function must have its own process to extract the field name, otherwise the function
     * name is returned instead.
     *
     * @param functionSelector  function for which associated name is being retrieved
     * @return                              field name associated to the function or the function's name if no field name is associated to that specific function
     */
    private static String getFunctionSelectorFieldName (FunctionSelector functionSelector){
        if (SUB_FIELD_FUNCTION.equalsIgnoreCase(functionSelector.getFunctionName())){
            return getSubFieldFunctionSelectorFieldName(functionSelector);
        }
        return functionSelector.getFunctionName();
    }

    /**
     * Method that retrieves the name associated to sub field functions. It builds the field name by concatenating the base field (First function parameter) and the
     * analyzer associated to that field (Second function parameter) separated by a colon.
     *
     * @param subFieldFunctionSelector
     * @return
     */
    private static String getSubFieldFunctionSelectorFieldName (FunctionSelector subFieldFunctionSelector){
        // Retrieves the base field name
        String field = subFieldFunctionSelector.getFunctionColumns().getSelectorList().get(0).getColumnName().getName();
        // Retrieves the analyzer name
        String subField = subFieldFunctionSelector.getFunctionColumns().getSelectorList().get(1).getStringValue();
        // Builds field name
        return field + "." + subField;
    }



    /**
     * Return if the Selector is a function, and if the function name is the passed.
     *
     * @param selector
     * @param functionName
     * @return
     */
    public static boolean isFunction(Selector selector, String... functionName) {

        if (!(selector instanceof FunctionSelector))
            return false;

        FunctionSelector functionSelector = (FunctionSelector) selector;
        return ArrayUtils.contains(functionName, functionSelector.getFunctionName().toLowerCase().toString());
    }


    public static boolean hasFunction(Map<Selector, String> columnMetadata, String... functionName) {

        for (Selector selector : columnMetadata.keySet()) {
            if (isFunction(selector, functionName)) {
                return true;
            }
        }

        return false;
    }

    public static FunctionSelector getFunctionSelector(Map<Selector, String> columnMetadata, String functionName) {

        for (Selector selector : columnMetadata.keySet()) {
            if (isFunction(selector, functionName)) {
                return (FunctionSelector) selector;
            }
        }

        return null;
    }

    public static String calculateSubFieldName(Selector selector) {
        FunctionSelector functionSelector = (FunctionSelector) selector;
        String field = functionSelector.getFunctionColumns().getSelectorList().get(0).getColumnName().getName();
        String subField = functionSelector.getFunctionColumns().getSelectorList().get(1).getStringValue();
        return field +"."+subField;
    }


}
