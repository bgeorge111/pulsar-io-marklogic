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

# URI generation strategies
MarkLogic Uris are to be unique. If a conflicting URI is generated, there would be silent overwrite of documents. If the conflicting URIs are in the same batch (when you have dmsdkBatchSize > 1), then there would be errors generated due to conflicting updates. You have a few options listed below to choose your URI generation strategy. Choose the option that best fits you for generating unique URIs. The final URI generated will be 
{mlDocumentURIPrefix}/{ID generated from mlIdStrategyForURI}/{mlDocumentURISuffix}
| Value      | Description | Example | 
| ---------- | ----------- | ------- |
| UUID | A system generated unique identifier. If you are not sure about your data, then this will be your best option. This is also the default option. | |
| JSONPATH | Choose a qualified JSON path for generating the ID. | If your document is <br> `{ "Customer" : { "id" : 10001, "name" : "Tim"}}` <br> and if the configuration properties are set as  `"mlDocumentURIPrefix" : "/pulsar-data"`<br> `"mlDocumentURISuffix" : ".json"` <br> `"mlIdStrategyForURI" : "JSONPATH"` <br> `"mlIdStrategyPath" : "/Customer/id"`, the URI that would be generated will be  <br> `/pulsar-data/10001.json`. <br> Note that if "id" is not unique, URIs generated will not be unique. If multiple JSON Paths are provided, then only the first one will be used. | 
| HASH | A MD5 hash of the values from all the JSON Paths provided. |  If your document is <br> `{ "Customer" : { "id" : 10001, "name" : "Tim"}}` <br> and if the configuration properties are set as  `"mlDocumentURIPrefix" : "/pulsar-data"`<br> `"mlDocumentURISuffix" : ".json"` <br> `"mlIdStrategyForURI" : "HASH"` <br> `"mlIdStrategyPath" : "/Customer/id, /Customer/name"`, the URI that would be generated will be  <br> `/pulsar-data/eba24f0e1c9e3363f0c4f5bcb3317946.json`. <br> Here `eba24f0e1c9e3363f0c4f5bcb33179461` is the MD5 hash value of `10001Tim`.  Note that if hashed value is not unique, URIs generated will not be unique. Multiple JSON Paths can be used, separated by comma
| PULSAR_META_WITH_SLASH | The URI generated will be having the topic, partitition and sequence number of the message. | For example, if the pulsar topic is persistent://public/default/marklogic-topic, the partition is marklogic-topic-0 and sequence number of message is 48792, and the below configuration properties are set  `"mlDocumentURIPrefix" : "/pulsar-data"`<br> `"mlDocumentURISuffix" : ".json"` <br> `"mlIdStrategyForURI" : "PULSAR_META_WITH_SLASH"` then the URI of the message is <br>`/pulsar-data/persistent/public/default/marklogic-topic/marklogic-topic-0/48792.json` <br> This strategy should ideally generate unique URIs, but note that it depends on the Pulsar's capability to generate unique sequence numbers within in a topic partition. Note that the `persistent://` in topic's URI is changed to `persistent/` |
| PULSAR_META_HASHED | The URI generated will be the hashed value of topic, partition and sequence number. |  For example, if the pulsar topic is `persistent://public/default/marklogic-topic`, the partition is `marklogic-topic-0` and sequence number of message is `48792`, and the below configuration properties are set  `"mlDocumentURIPrefix" : "/pulsar-data"`<br> `"mlDocumentURISuffix" : ".json"` <br> `"mlIdStrategyForURI" : "PULSAR_META_HASHED"` <br> then the URI of the message is <br>`/pulsar-data/9cdfc945708835835da746a35c5a7fca.json` <br> where `9cdfc945708835835da746a35c5a7fca` is the MD5 hash value of `persistent://public/default/marklogic-topicmarklogic-topic-048792` <br> This strategy should ideally generate unique URIs, but note that it depends on the Pulsar's capability to generate unique sequence numbers within in a topic partition.


# Example
	{"configs" : {
	"mlConnectionHost" :  "mydomainpart1.mydomainpart2.a.marklogicsvc.com",
	"mlConnectionPort" :  8010,
	"mlDatabase" :  "data-hub-STAGING",
	"mlSecurityContext" :  "BASIC",
	"mlUserName" :  "dh-admin-1",
	"mlPassword" :  "mypwd",
	"mlConnectionType" :  "GATEWAY",
	"mlSimpleSSL" :  "true",
	"mlPathToCertFile" :  "",
	"mlPasswordForCertFile" :  "",
	"mlExternalName" :  "",
	"mlHostNameVerifier" :  "ANY",
	"mlTlsVersion" :  "TLSv1.2",
	"mlSSLMutualAuth" :  "false",
	"dmsdkBatchSize" :  1,
	"dmsdkThreadCount" :  10,
	"dmsdkTransform" :  "",
	"dmsdkTransformParams" :  "",
	"mlDocumentCollections" :  "coll-1wayauth-strict",
	"mlDocumentFormat" :  "JSON",
	"mlDocumentMimeType" :  "",
	"mlDocumentPermissions" :  "rest-reader,read,rest-writer,update",
	"mlDocumentURIPrefix" :  "/pulsar-data/1wayauth/strict/",
	"mlDocumentURISuffix" :  ".json",
	"mlIdStrategyForURI" : "JSONPATH",
	"mlIdStrategyPath" : "/CustomerInfo/key",
	"mlAddTopicAsCollections" : "true"
	}
	}
