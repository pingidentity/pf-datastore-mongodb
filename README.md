#pf-datastore-mongodb

### Overview

PingFederate custom data store for MongoDB. Allows PingFederate to retrieve attributes from a MongoDB data source.


### System Requirements / Dependencies

Requires:
 - PingFederate 7.2.x or higher
 - Mongo Java driver
 - Apache Commons logging

 
### Installation
 
1. Compile the plugin in (refer to the [PingFederate server SDK documentation] for details on compiling PingFederate plug-ins)
2. Copy the resulting .jar file to the <pf_home>/server/default/deploy folder (on all nodes and admin instances).
3. Restart PingFederate
 
[PingFederate server SDK documentation]: http://documentation.pingidentity.com/display/PF/SDK+Developer%27s+Guide


### Configuration

1. Add a new data store under: System Settings > Data Stores
2. Select the "Custom" data store type
3. Name the instance and select "MongoDB Data Store" as the type
4. Refer to the inline documentation to configure your MongoDB instance details

You can now use the MongoDB data store to retrieve attributes during an attribute lookup. For example retrieving attributes for an OAuth 2.0 access token contract.


### Disclaimer

This software is open sourced by Ping Identity but not supported commercially as such. Any questions/issues should go to the mailing list, the Github issues tracker or the author pmeyer@pingidentity.com directly See also the DISCLAIMER file in this directory.