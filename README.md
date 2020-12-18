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
| ----------- | ----------- |
| mlConnectionHost    | true       | The MarkLogic host that the connector connects to



# Example
