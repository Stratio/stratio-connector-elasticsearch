## CONCEPTS

### Settings and Analyzers
Elasticsearch index behaviour can be customized via settings [properties]
(https://www.elastic.co/guide/en/elasticsearch/guide/current/_index_settings.html). We can set these properties during the catalog
creation using the option "settings" including the configuration as a json string. This way we can set properties like the number of
shards and replicas and even define new [analyzers] (https://www.elastic.co/guide/en/elasticsearch/reference/1.6/analysis.html).


    CREATE CATALOG movies WITH {'settings' : '{"number_of_replicas" : "0","number_of_shards" : "1","analysis" : {"analyzer" : {"raw" :
    {"tokenizer" : "keyword"},"normalized" : {"filter" : ["lowercase"],"tokenizer" : "keyword"},"english" : {"filter" : ["lowercase",
    "english_stemmer"],"tokenizer" : "standard"}},"filter" : {"english_stemmer" : {"type" : "stemmer","language" : "english"}}}}'};


In this example we are  configuring three analyzers appart form the standard ones:

* **raw**: Does not tokenize nor change the original nor lowercases it. Suitable for groupings and aggregations if the fields are
case sensitive.
* **normalized**: Lowercases the input but does not tokenize it. Suitable for sorting (and groupings and aggregations if fields are
not case sensitive).
* **english**: Applies the same transformations than the standard analyzer and applies english stemming to the resulting tokens.
Suitable for searching through english texts.

Take into account that this is an advanced functionality that requires some elasticsearch knowledge, if you just want to use the
basic search you can just create the catalog with no options.



### SubFields
Elasticsearch allows us to define subfields by applying different analyzers to the original field. Those fields may be used for
specific functionalities like sorting the results, getting agggregations or even achieving a better search experience by applying,
for example, language specific rules.

#### Defining SubFields
Those subfields can be defined through the mappings configuration and once the subfields had been set, the subfields associated
to each field would be generated automatically each time a new document is inserted. In order to create this mapping configuration
we just have to specify the analyzers that will apply to each field during the table creation, and the appropriate configuration
will be generated. Keep in mind that, these subfields can only be set for text fields and that, for analyzers different from the
standard ones you should define them through the settings property during catalog creation.
The standard analyzer is always applied to the original field unless it is specified as "not_analyzed", therefore even if no subfield
is defined we could have at least a basic analysis applied to our text fields. Standard analyzer tokenizes the input by whitespaces
and other punctuation symbols and lowercases the resulting tokens being suitable for generic language texts.

    CREATE TABLE movie ON CLUSTER elasticsearch_cluster (id text("index":"not_analyzed") PRIMARY KEY, title text("analyzer":"english",
    "analyzer":"normalized", "analyzer":"raw"), released boolean);

In this example we are creating three additional subfields associated to the field "title", therefore we will have the original
field "title" using standard analyzer, and the subfields "title.raw", "title.normalized" and "title.english" using the associated
analyzers.

####Using SubFields
In order to work with those subfields we can use the "SUB_FIELD" function or the subfield notation depending on whether we are using
them inside a function or not:

* **Outside a function**: We must call the "SUB_FIELD" function passing the field name as the first parameter and the name of the
analyzer as the second one. (e.g. "sub_field(field, "analyzer")"
* **Inside a fuction**: We must use the subfield notation being "{field name}.{analyzer}" (e.g. "function(field.analizer)")