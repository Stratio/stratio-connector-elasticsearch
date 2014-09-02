package com.stratio.connector.elasticsearch.core.configuration;

/**
 * Created by jmgomez on 1/09/14.
 */
public enum ConfigurationOptions {


    NODE_TYPE("node_type","false"),
    NODE_DATA("node.data","false"),
    NODE_MASTER("node.master","false"),
    TRANSPORT_SNIFF("client.transport.sniff","true"),
    CLUSTER_NAME("cluster.name","cluster_name"),
    HOST("Hosts",new String[]{"localhost"}),
    PORT("Port",new String[]{"9300"});



    private final String optionName;
    private final String[] defaultValue;

    public String[] getDefaultValue() {
        return defaultValue;
    }

    public String getOptionName() {
        return optionName;
    }


    ConfigurationOptions(String optionName, String... defaultValue) {
        this.optionName = optionName;
        this.defaultValue = defaultValue;

    }


}
