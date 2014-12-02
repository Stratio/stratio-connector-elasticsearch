# Fist Steps #

Here is a group of examples to illustrate the the Elasticsearch Crossdata connector use.

## Connector Configuration ##
First of all [Stratio Crossdata 0.1.1] https://github.com/Stratio/crossdata.git is needed and must be installed. The 
server and shell must be running.

In Crossdata Shell we need to add the Datastore Manifest.

```
   > add datastore "<path_to_manifest_folder>/ElasticSearchDataStore.xml";
```

The output must be:

```
   [INFO|Shell] Response time: 0 seconds    
   [INFO|Shell] OK
```

Now we need to add the ConnectorManifest.

```
   > add connector "<path_to_manifest_folder>/ElasticSearchConnector.xml";  
```
The output must be:


```
   [INFO|Shell] Response time: 0 seconds    
   [INFO|Shell] OK
```

At this point we have informed to crossdata the connector options and operations. Now we go to configure the 
datastore cluster.

```
> ATTACH CLUSTER elasticsearchCluster ON DATASTORE elasticsearch WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]', 
'Port': '[Port1,Port2,...,Portn]'};
```

The output must be:
```
  Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
  Cluster attached successfully
```

Now we must run the connector

```
  > <path_to_connector_executable>/connector-elasticsearch-core-<version> start
```

To ensure that the connector is online we can execute the crossdata shell command

```
  > describe connectors;
```

And the output mus be

```
Connector: connector.elasticsearchconnector	ONLINE	[]	[datastore.elasticsearch]	akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/
```

The last step is to attach the connector to the cluster created before.

```
  >  ATTACH CONNECTOR elasticsearchconnector TO elasticsearchCluster  WITH OPTIONS {};
```

The output must be
```
CONNECTOR attached successfully
```

## Create Catalog and Table  ##

Now we'll create the catalog and table which use later in the next steps.

To create catalog we must execute.

```
    > CREATE CATALOG highschool;
```
The output must be.

```
CREATE CATALOG highschool;
```

To create the table we must execute the next command.


# Getting started #
Here is an example of Crossdata with a Cassandra Connector as an access to a Cassandra data store.

First of all [Stratio Cassandra](https://github.com/Stratio/stratio-cassandra) is needed and must be installed and running.

At this point Crossdata server must be running and it is need to start Crossdata shell. In this shell we will create
a new catalog, with a new table, and after we will make a Select query.

Now, we need the [Cassandra Connector](https://github.com/Stratio/stratio-connector-cassandra), install it:

```
    > mvn crossdata-connector:install
```
And then, run it:

```
    > target/stratio-connector-cassandra-0.1.0/bin/stratio-connector-cassandra-0.1.0 start
```

**NOTE:** All the connectors have to be started once CrossdataServer is already running!

Now, from the Crossdata Shell we can write the following commands:

Add a data store. We need to specified the XML manifest that defines the data store. The XML manifest can be found
in the path of the Cassandra Connector in target/stratio-connector-cassandra-0.1.0/conf/CassandraDataStore.xml

```
    xdsh:user>  ADD DATASTORE <Absolute path to Cassandra Datastore manifest>;
```

Attach cluster on that data store. The data store name must be the same as the defined in the data store manifest.

```
    xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<ipHost_1,
  ipHost_2,...ipHost_n>]', 'Port': <cassandra_port>};
```

Add the connector manifest. The XML with the manifest can be found in the path of the Cassandra Connector in
target/stratio-connector-cassandra-0.1.0/conf/CassandraConnector.xml

```
    xdsh:user>  ADD CONNECTOR <Path to Cassandra Connector Manifest>;
```

Attach the connector to the previously defined cluster. The connector name must match the one defined in the
Connector Manifest, and the cluster name must match with the previously defined in the ATTACH CLUSTER command.

```
    xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {'DefaultLimit': '1000'};
```

At this point, we can start to send queries, that Crossdata execute with the connector specified.


    xdsh:user> CREATE CATALOG catalogTest;

    xdsh:user> USE catalogTest;

    xdsh:user> CREATE TABLE tableTest ON CLUSTER <cluster name> (id int PRIMARY KEY, name text);

    xdsh:user> INSERT INTO tableTest(id, name) VALUES (1, 'stratio');

    xdsh:user> SELECT * FROM tableTest;
