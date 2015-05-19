About
*****

Stratio Connector Elasticsearch is a crossdata connector interface
implementation for elasticsearch 1.3.2.

Requirements
------------

Install [elasticsearch 1.3.2]
(http://www.elasticsearch.org/downloads/1-3-2/) and run it. [Crossdata]
(https://github.com/Stratio/crossdata) is needed to interact with this
connector.

Compiling Stratio Connector Elasticsearch
-----------------------------------------

To automatically build execute the following command:

::

       > mvn clean compile install

Build an executable Connector Elasticsearch
-------------------------------------------

To generate the executable execute the following command:

::

       > mvn crossdata-connector:install

Running the Stratio Connector Elasticsearch
-------------------------------------------

To run Connector Elasticsearch execute:

::

       > cd connector-elasticsearch-core/
       > target/connector-elasticsearch-core-0.4.0-SNAPSHOT/bin/connector-elasticsearch-core-0.4.0-SNAPSHOT start

To stop the connector execute:

::

       > target/connector-elasticsearch-core-0.4.0-SNAPSHOT/bin/connector-elasticsearch-core-0.4.0-SNAPSHOT stop

Build a redistributable package
-------------------------------
It is possible too, to create a RPM or DEB redistributable package.

RPM Package:

::

       > mvn unix:package-rpm -N
    
DEB Package:

::
   
       > mvn unix:package-deb -N

Once the package it's created, execute this commands to install:

RPM Package:
 
::   
    
       > rpm -i target/stratio-connector-elasticsearch-0.4.0-SNAPSHOT.rpm
     
DEB Package:

::   
    
       > dpkg -i target/stratio-connector-elasticsearch-0.4.0-SNAPSHOT.deb

Now to start/stop the connector:
 
::   
    
       > service stratio-connector-elasticsearch start
       > service stratio-connector-elasticsearch stop

How to use Elasticsearch Connector
----------------------------------

1. Start `crossdata-server and then
   crossdata-shell <https://github.com/Stratio/crossdata>`__.
2. Start Elasticsearch Connector as it is explained before
3. In crossdata-shell:

   Add a data store. We need to specified the XML manifest that defines
   the data store. The XML manifest can be found in the path of the
   Elasticsearch Connector in
   target/stratio-connector-elasticsearch-0.4.0-SNAPSHOT/conf/ElasticSearchDataStore.xml

   ``xdsh:user>  ADD DATASTORE <Absolute path to Streaming Datastore manifest>;``

   Attach cluster on that datastore. The datastore name must be the same
   as the defined in the Datastore manifest.

   ``xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<IPHost_1,IPHost_2,...,IPHost_n>]', 'Port': '[<PortHost_1,PortHost_2,...,PortHost_n>]'};``

   Add the connector manifest. The XML with the manifest can be found in
   the path of the Cassandra Connector in
   target/-connector-elasticsearch-core-0.4.0-SNAPSHOT/conf/ElasticSearchConnector.xml

   ``xdsh:user>  ADD CONNECTOR <Path to Elasticsearch Connector Manifest>``

   Attach the connector to the previously defined cluster. The connector
   name must match the one defined in the Connector Manifest, and the
   cluster name must match with the previously defined in the ATTACH
   CLUSTER command.

   ::

       ```
           xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};
       ```

   At this point, we can start to send queries, that Crossdata execute
   with the connector specified.

   ::

       ...
           xdsh:user> CREATE CATALOG catalogTest;

           xdsh:user> USE catalogTest;

           xdsh:user> CREATE TABLE tableTest ON CLUSTER <cluster_name> (id int PRIMARY KEY, name text);

           xdsh:user> INSERT INTO tableTest(id, name) VALUES (1, 'stratio');

           xdsh:user> SELECT * FROM tableTest;
       ...

License
=======

Licensed to STRATIO (C) under one or more contributor license
agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The STRATIO (C)
licenses this file to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
