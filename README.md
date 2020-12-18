# pulsar-io-marklogic
This is a basic Sink connector that pushes messages from Apache Pulsar topic to MarkLogic database. The connector is using MarkLogic DMSDK java API at its core. 

# Requirements 
1. MarkLogic 9+ 
2. Apache Pulsar 2.7.0+ 
3. Maven (if building locally) 
4. Java 1.8 

# To build locally
 
1. Download or fork the source code
2. At the project root, run mvn clean install 
3. The archive file (.nar) will be created in the `<root>/target` folder 

*If you do not want to build locally, the archive file .nar file can be downloaded and installed. See the instructions below.*

# Install the connector and Run
1. Download the .nar archive file
2. Copy to a convenient location where pulsar is installed or running (Ex. <pulsar installation folder>/connectors 
3. Build the json configuration file marklogic-sink.json 
4. (Optional) If there is a MarkLogic connector that is already running, delete using
 
   `bin/pulsar-admin sinks delete --name <connector name>`
5. Create the new connector using below command 

	bin/pulsar-admin sinks create --name <connector name> --archive <path to the .nar archive file> --inputs <source pulsar topic> --sink-config-file <path to the configuration file> --processing-guarantees EFFECTIVELY_ONCE

*There are other options for pulsar-admin. Please see apache pulsar documentation for details.*

# Configuring Sink Connector

| Property      | Required | Description | 
| ----------- | ----------- | ---------- |
| mlConnectionHost    | true       | The MarkLogic host that the connector connects to | 
| mlConnectionPort    | true       | The MarkLogic app server port that the connector connects to | 
| mlDatabase    | true       | The MarkLogic database that the connector connects to | 
| mlSecurityContext    | true       | The MarkLogic Security Context to create - digest, basic, certificate | 
| mlUserName    | true       | Name of MarkLogic user to authenticate as | 
| mlPassword    | true       | Password for the MarkLogic user | 
| mlConnectionType    | true       | Connection Type; DIRECT or GATEWAY | 
| mlSimpleSSL    | false       | Set to true to use a trust-everything SSL connection | 
| mlPathToCertFile    | false       | Path to the certificate file for certificate authentication | 
| mlPasswordForCertFile    | false       | Password for the certificate file | 
| mlExternalName    | false       | External name for Kerberos authentication | 
| dmsdkBatchSize    | false       | Number of documents to write in each batch of DMSDK. Default is 1 | 
| dmsdkThreadCount    | false       | Number of threads for DMSDK to use. Default is 1 | 
| dmsdkTransform    | false       | Name of a REST transform to use when writing documents | 
| dmsdkTransformParams    | false       | Delimited set of transform parameter names and values | 
| dmsdkTransformParamDelimiter    | false       | Delimiter for transform parameter names and values; defaults to a comma | 
| mlAddTopicAsCollections    | false       | Indicates if the topic name should be added to the set of collections for a document | 
| mlDocumentCollections    | false       | Comma delimited collections to add each document to | 
| mlDocumentFormat    | true       | Defines format of each document; can be one of json, xml, text, binary, or unknown | 
| mlDocumentMimeType    | false       | Defines the mime type of each document; typically format is set instead of mime type | 
| mlDocumentPermissions    | false       | Comma delimited permissions to add to each document; role1,capability1,role1,capability2,role2,capability1 | 
| mlDocumentURIPrefix    | false       | Prefix to prepend to each generated URI | 
| mlDocumentURISuffix    | false       | Suffix to append to each generated URI |
| mlSSL    | false       | Whether a custom SSL connection to the App server like mutual Auth. The dependency on this will be eliminated in future| 
| mlHostNameVerifier    | false       | The strictness of Host Verifier - ANY, COMMON, STRICT | 
| mlSSLMutualAuth    | false       | Mutual Authentication for Basic or Digest: true or false | 
| mlIdStrategyForURI    | false       | The ID Strategy for URI. UUID,JSONPATH,HASH,PULSAR_META_WITH_SLASH, PULSAR_META_HASHED. Default is UUID| 
| mlIdStrategyPath    | false       | The JSON path for ID Strategy |






# Example
