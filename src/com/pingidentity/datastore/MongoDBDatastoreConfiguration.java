package com.pingidentity.datastore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.sourceid.saml20.adapter.conf.Configuration;
import org.sourceid.saml20.adapter.conf.Row;
import org.sourceid.saml20.adapter.conf.Table;
import org.sourceid.saml20.adapter.gui.AdapterConfigurationGuiDescriptor;
import org.sourceid.saml20.adapter.gui.RadioGroupFieldDescriptor;
import org.sourceid.saml20.adapter.gui.SelectFieldDescriptor;
import org.sourceid.saml20.adapter.gui.TableDescriptor;
import org.sourceid.saml20.adapter.gui.TextFieldDescriptor;
import org.sourceid.saml20.adapter.gui.validation.ConfigurationValidator;
import org.sourceid.saml20.adapter.gui.validation.ValidationException;
import org.sourceid.saml20.adapter.gui.validation.impl.IntegerValidator;
import org.sourceid.saml20.adapter.gui.validation.impl.RequiredFieldValidator;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.pingidentity.sources.CustomDataSourceDriverDescriptor;
import com.pingidentity.sources.SourceDescriptor;
import com.pingidentity.sources.gui.FilterFieldsGuiDescriptor;


public class MongoDBDatastoreConfiguration {
	
	private static final String DATA_SOURCE = "MongoDB Data Store v 1.0";
	private static final String DATA_SOURCE_CONFIG_DESC = "Configuration settings for the MongoDB data store";

    private static final String CONFIG_SERVER_LIST_TABLE = "MongoDB Server List";
	private static final String CONFIG_SERVER_LIST_TABLE_DESC = "List of MongoDB servers (you must define a minimum of one server)";   

    private static final String CONFIG_SERVER = "Server";
	private static final String CONFIG_SERVER_DESC = "Server hostname or IP address";   
    private static final String CONFIG_SERVER_DEFAULT_VALUE = "localhost";

    private static final String CONFIG_PORT = "Port";
	private static final String CONFIG_PORT_DESC = "MongoDB listening port";   
	private static final String CONFIG_PORT_DEFAULT_VALUE = "27017";

    private static final String CONFIG_DATABASE = "Database Name";
    private static final String CONFIG_DATABASE_DESC = "Database instance name";

    private static final String CONFIG_COLLECTION = "Collection Name";
    private static final String CONFIG_COLLECTION_DESC = "Collection name";

    private static final String CONFIG_AUTH_METHOD = "Authentication Method";
	private static final String CONFIG_AUTH_METHOD_DESC = "Authentication Method";   

    private static final String CONFIG_AUTH_METHOD_VALUE_NONE = "None";
	private static final String CONFIG_AUTH_METHOD_VALUE_USERNAME = "Username / Password";   

    private static final String CONFIG_AUTH_CREDENTIAL_USERNAME = "Username";
	private static final String CONFIG_AUTH_CREDENTIAL_USERNAME_DESC = "Database authentication username (required if authentication method is username/password)";   

    private static final String CONFIG_AUTH_CREDENTIAL_PASSWORD = "Password";
	private static final String CONFIG_AUTH_CREDENTIAL_PASSWORD_DESC = "Database authentication password (required if authentication method is username/password)";   
	
    private static final String CONFIG_ADVANCED_READ_PREFERENCE = "Read Preference";
	private static final String CONFIG_ADVANCED_READ_PREFERENCE_DESC = "Read preference setting";   

	private static final String CONFIG_ADVANCED_READ_PREFERENCE_VALUE_PRIMARY = "PRIMARY";
	private static final String CONFIG_ADVANCED_READ_PREFERENCE_VALUE_PRIMARY_PREFERRED = "PRIMARY PREFERRED";
    private static final String CONFIG_ADVANCED_READ_PREFERENCE_VALUE_SECONDARY = "SECONDARY";
    private static final String CONFIG_ADVANCED_READ_PREFERENCE_VALUE_SECONDARY_PREFERRED = "SECONDARY PREFERRED";
    private static final String CONFIG_ADVANCED_READ_PREFERENCE_VALUE_NEAREST = "NEAREST";

    private static final String CONFIG_ADVANCED_ACTION_TIMEOUT = "Action Timeout";
	private static final String CONFIG_ADVANCED_ACTION_TIMEOUT_DESC = "Timeout per action (ms)";   
    private static final String CONFIG_ADVANCED_ACTION_TIMEOUT_DEFAULT_VALUE = "5000";

    private static final String CONFIG_ADVANCED_CONNECT_TIMEOUT = "Connect Timeout";
	private static final String CONFIG_ADVANCED_CONNECT_TIMEOUT_DESC = "Database connection timeout (ms)";   
    private static final String CONFIG_ADVANCED_CONNECT_TIMEOUT_DEFAULT_VALUE = "5000";
	
    private static final String CONFIG_ADVANCED_WRITE_CONCERN = "Write Concern";
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_DESC = "Write concern method";   

	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_ACKNOWLEDGED = "ACKNOWLEDGED";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_FSYNC_SAFE = "FSYNC_SAFE";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_FSYNCHED = "FSYNCHED";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_JOURNAL_SAFE = "JOURNAL_SAFE";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_JOURNALED = "JOURNALED";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_MAJORITY = "MAJORITY";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_NORMAL = "NORMAL";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_REPLICA_ACKNOWLEDGED = "REPLICA_ACKNOWLEDGED";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_REPLICAS_SAFE = "REPLICAS_SAFE";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_SAFE = "SAFE";   
	private static final String CONFIG_ADVANCED_WRITE_CONCERN_VALUE_UNACKNOWLEDGED = "UNACKNOWLEDGED";   
	
    private static final String CONFIG_FILTER = "Datastore filter";
    private static final String CONFIG_FILTER_DESC = "Filter to use to find the database entry (ie uid=${username}) - Note: Currently only supports attribute=value.";
    
    // initialise a logger
    private Log log = LogFactory.getLog(this.getClass());

    // initialise configuration object
    protected Configuration configuration = null;

    // initialise configuration variables (private)
    private MongoClient mongoClient;
    private DB mongoDB;

    // initialise configuration variables (protected)
    protected int actionTimeout;
    protected ReadPreference readPreference;
    protected DBCollection mongoCollection;
    protected String filterKey;

    public SourceDescriptor getSourceDescriptor(MongoDBDatastore ds)
    {
        // create the configuration descriptor for our custom data store
        AdapterConfigurationGuiDescriptor dataStoreConfigGuiDesc = new AdapterConfigurationGuiDescriptor(DATA_SOURCE_CONFIG_DESC);

        // Create the table to hold the list of servers
        TableDescriptor serverListTableDescriptor = new TableDescriptor(CONFIG_SERVER_LIST_TABLE, CONFIG_SERVER_LIST_TABLE_DESC);
		TextFieldDescriptor serverNameDescriptor = new TextFieldDescriptor(CONFIG_SERVER, CONFIG_SERVER_DESC);
		TextFieldDescriptor serverPortDescriptor = new TextFieldDescriptor(CONFIG_PORT, CONFIG_PORT_DESC);
		serverNameDescriptor.addValidator(new RequiredFieldValidator());
		serverNameDescriptor.setDefaultValue(CONFIG_SERVER_DEFAULT_VALUE);
		serverListTableDescriptor.addRowField(serverNameDescriptor);
		serverPortDescriptor.addValidator(new RequiredFieldValidator());
		serverPortDescriptor.addValidator(new IntegerValidator(0, 65537));
		serverPortDescriptor.setDefaultValue(CONFIG_PORT_DEFAULT_VALUE);
		serverListTableDescriptor.addRowField(serverPortDescriptor);
        dataStoreConfigGuiDesc.addTable(serverListTableDescriptor);
        
		// Radio group of authentication methods
		String[] authenticationMethodOptions = new String[] { CONFIG_AUTH_METHOD_VALUE_NONE, CONFIG_AUTH_METHOD_VALUE_USERNAME };
        RadioGroupFieldDescriptor authenticationMethodDescriptor = new RadioGroupFieldDescriptor(CONFIG_AUTH_METHOD, CONFIG_AUTH_METHOD_DESC, authenticationMethodOptions);
        authenticationMethodDescriptor.setDefaultValue(CONFIG_AUTH_METHOD_VALUE_NONE);
        dataStoreConfigGuiDesc.addField(authenticationMethodDescriptor);

		TextFieldDescriptor authUsernameDescriptor = new TextFieldDescriptor(CONFIG_AUTH_CREDENTIAL_USERNAME, CONFIG_AUTH_CREDENTIAL_USERNAME_DESC);
        dataStoreConfigGuiDesc.addField(authUsernameDescriptor);

        TextFieldDescriptor authPasswordDescriptor = new TextFieldDescriptor(CONFIG_AUTH_CREDENTIAL_PASSWORD, CONFIG_AUTH_CREDENTIAL_PASSWORD_DESC, true);
        dataStoreConfigGuiDesc.addField(authPasswordDescriptor);
        
		TextFieldDescriptor databaseNameDescriptor = new TextFieldDescriptor(CONFIG_DATABASE, CONFIG_DATABASE_DESC);
		databaseNameDescriptor.addValidator(new RequiredFieldValidator());
        dataStoreConfigGuiDesc.addField(databaseNameDescriptor);

        TextFieldDescriptor collectionNameDescriptor = new TextFieldDescriptor(CONFIG_COLLECTION, CONFIG_COLLECTION_DESC);
        collectionNameDescriptor.addValidator(new RequiredFieldValidator());
        dataStoreConfigGuiDesc.addField(collectionNameDescriptor);

        
        // Add the advanced settings
		String[] readPreferenceOptions = new String[] { CONFIG_ADVANCED_READ_PREFERENCE_VALUE_PRIMARY, 
				CONFIG_ADVANCED_READ_PREFERENCE_VALUE_PRIMARY_PREFERRED, 
				CONFIG_ADVANCED_READ_PREFERENCE_VALUE_SECONDARY,
				CONFIG_ADVANCED_READ_PREFERENCE_VALUE_SECONDARY_PREFERRED,
				CONFIG_ADVANCED_READ_PREFERENCE_VALUE_NEAREST };
		SelectFieldDescriptor readPreferenceDescriptor = new SelectFieldDescriptor(CONFIG_ADVANCED_READ_PREFERENCE, CONFIG_ADVANCED_READ_PREFERENCE_DESC, readPreferenceOptions);
		readPreferenceDescriptor.setDefaultValue(CONFIG_ADVANCED_READ_PREFERENCE_VALUE_PRIMARY_PREFERRED);
        dataStoreConfigGuiDesc.addAdvancedField(readPreferenceDescriptor);
		
		String[] writeConcernOptions = new String[] { CONFIG_ADVANCED_WRITE_CONCERN_VALUE_ACKNOWLEDGED, 
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_FSYNC_SAFE,
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_FSYNCHED,   
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_JOURNAL_SAFE,   
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_JOURNALED,   
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_MAJORITY,   
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_NORMAL,   
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_REPLICA_ACKNOWLEDGED,   
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_REPLICAS_SAFE,   
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_SAFE,
				CONFIG_ADVANCED_WRITE_CONCERN_VALUE_UNACKNOWLEDGED };
		SelectFieldDescriptor writeConcernDescriptor = new SelectFieldDescriptor(CONFIG_ADVANCED_WRITE_CONCERN, CONFIG_ADVANCED_WRITE_CONCERN_DESC, writeConcernOptions);
		writeConcernDescriptor.setDefaultValue(CONFIG_ADVANCED_WRITE_CONCERN_VALUE_ACKNOWLEDGED);
        dataStoreConfigGuiDesc.addAdvancedField(writeConcernDescriptor);

		TextFieldDescriptor connectTimeoutDescriptor = new TextFieldDescriptor(CONFIG_ADVANCED_CONNECT_TIMEOUT, CONFIG_ADVANCED_CONNECT_TIMEOUT_DESC);
		connectTimeoutDescriptor.setDefaultValue(CONFIG_ADVANCED_CONNECT_TIMEOUT_DEFAULT_VALUE);
		connectTimeoutDescriptor.addValidator(new IntegerValidator(0, 3600000));
        dataStoreConfigGuiDesc.addAdvancedField(connectTimeoutDescriptor);
		
		TextFieldDescriptor actionTimeoutDescriptor = new TextFieldDescriptor(CONFIG_ADVANCED_ACTION_TIMEOUT, CONFIG_ADVANCED_ACTION_TIMEOUT_DESC);
		actionTimeoutDescriptor.setDefaultValue(CONFIG_ADVANCED_ACTION_TIMEOUT_DEFAULT_VALUE);
		actionTimeoutDescriptor.addValidator(new IntegerValidator(0, 3600000));
        dataStoreConfigGuiDesc.addAdvancedField(actionTimeoutDescriptor);
		
       
        // Add the configuration field for the search Filter
        FilterFieldsGuiDescriptor filterFieldsDescriptor = new FilterFieldsGuiDescriptor();
        filterFieldsDescriptor.addField(new TextFieldDescriptor(CONFIG_FILTER, CONFIG_FILTER_DESC));
        
        
        // Perform final validation
        ConfigurationValidator serverListTableValidator = new ConfigurationValidator() {
        	@Override
        	public void validate(Configuration config) throws ValidationException {

        		// Validate that there is at least one server listed
        		Table serverListTable = config.getTable(CONFIG_SERVER_LIST_TABLE);
        		if (serverListTable.getRows().size() < 1) {
        			throw new ValidationException("You must define at least one MongoDB server");
        		}
        	}
        };

        ConfigurationValidator authenticationMethodValidator = new ConfigurationValidator() {
        	@Override
        	public void validate(Configuration config) throws ValidationException {
        		
        		// Validate that if the username/password method is checked, that we included a username and password
        		if (config.getFieldValue(CONFIG_AUTH_METHOD).equals(CONFIG_AUTH_METHOD_VALUE_USERNAME)) {
        			if (config.getFieldValue(CONFIG_AUTH_CREDENTIAL_USERNAME) == null || config.getFieldValue(CONFIG_AUTH_CREDENTIAL_USERNAME).isEmpty()) {
        				throw new ValidationException("Authentication method set to username/password but no username specified.");
        			}

        			if (config.getFieldValue(CONFIG_AUTH_CREDENTIAL_PASSWORD) == null || config.getFieldValue(CONFIG_AUTH_CREDENTIAL_PASSWORD).isEmpty()) {
        				throw new ValidationException("Authentication method set to username/password but no password specified.");
        			}
        		}
        	}
        };

        dataStoreConfigGuiDesc.addValidator(serverListTableValidator);
        dataStoreConfigGuiDesc.addValidator(authenticationMethodValidator);
        
        return new CustomDataSourceDriverDescriptor(ds, DATA_SOURCE, dataStoreConfigGuiDesc, filterFieldsDescriptor);
    }

    public void configure(final Configuration config)
    {
    	log.debug("---[ Configuring MongoDB Client ]------");
    	this.configuration = config;
    	MongoCredential dbCredential = null;
    	
    	// Set the authentication method if defined
    	if (configuration.getFieldValue(CONFIG_AUTH_METHOD).equals(CONFIG_AUTH_METHOD_VALUE_USERNAME)) {
        	dbCredential = MongoCredential.createMongoCRCredential(configuration.getFieldValue(CONFIG_AUTH_CREDENTIAL_USERNAME), configuration.getFieldValue(CONFIG_DATABASE), configuration.getFieldValue(CONFIG_AUTH_CREDENTIAL_PASSWORD).toCharArray());
    	}

	    // Set connect timeout
	    int connectTimeout = configuration.getIntFieldValue(CONFIG_ADVANCED_CONNECT_TIMEOUT);
	    MongoClientOptions options = new MongoClientOptions.Builder().connectTimeout(connectTimeout).build();
    	
    	// Get the list of servers
	    Table serverListTable = configuration.getTable(CONFIG_SERVER_LIST_TABLE);
	    List<Row> serverListRows = serverListTable.getRows();
	    
	    if (serverListRows != null) {
        	
	    	List<ServerAddress> mongoDBServerList = new ArrayList<ServerAddress>();

        	for (Row serverListRow : serverListRows) {
        		try {
        			mongoDBServerList.add(new ServerAddress(serverListRow.getFieldValue(CONFIG_SERVER), serverListRow.getIntFieldValue(CONFIG_PORT)));
        		}catch (UnknownHostException ex) {
        			log.error("ERROR: Unknown or invalid host " + serverListRow.getFieldValue(CONFIG_SERVER) + " - " + ex.getMessage());
        		}
	    	}
	    	
        	// initialise the MongoClient using credentials if defined
			if (dbCredential != null) {
				mongoClient = new MongoClient(mongoDBServerList, Arrays.asList(dbCredential), options);
			} else {
				mongoClient = new MongoClient(mongoDBServerList, options);
			}

	    } else { // no servers listed (should never get here due to configuration validation)

			log.error("ERROR: No database servers listed");
	    }

	    // Set write concern
	    switch (configuration.getFieldValue(CONFIG_ADVANCED_WRITE_CONCERN)) {
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_ACKNOWLEDGED:
	    	mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_FSYNC_SAFE:
	    	mongoClient.setWriteConcern(WriteConcern.FSYNC_SAFE);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_FSYNCHED:
	    	mongoClient.setWriteConcern(WriteConcern.FSYNCED);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_JOURNAL_SAFE:
	    	mongoClient.setWriteConcern(WriteConcern.JOURNAL_SAFE);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_JOURNALED:
	    	mongoClient.setWriteConcern(WriteConcern.JOURNALED);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_MAJORITY:
	    	mongoClient.setWriteConcern(WriteConcern.MAJORITY);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_NORMAL:
	    	mongoClient.setWriteConcern(WriteConcern.NORMAL);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_REPLICA_ACKNOWLEDGED:
	    	mongoClient.setWriteConcern(WriteConcern.REPLICA_ACKNOWLEDGED);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_REPLICAS_SAFE:
	    	mongoClient.setWriteConcern(WriteConcern.REPLICAS_SAFE);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_SAFE:
	    	mongoClient.setWriteConcern(WriteConcern.SAFE);
	    	break;
	    
	    case CONFIG_ADVANCED_WRITE_CONCERN_VALUE_UNACKNOWLEDGED:
	    	mongoClient.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
	    	break;
	    }
	    
	    // Store read preferenc
	    switch (configuration.getFieldValue(CONFIG_ADVANCED_READ_PREFERENCE)) {

    	case CONFIG_ADVANCED_READ_PREFERENCE_VALUE_PRIMARY:
    		readPreference = ReadPreference.primary();
    		break;
    
    	case CONFIG_ADVANCED_READ_PREFERENCE_VALUE_PRIMARY_PREFERRED:
    		readPreference = ReadPreference.primaryPreferred();
    		break;
    
    	case CONFIG_ADVANCED_READ_PREFERENCE_VALUE_SECONDARY:
    		readPreference = ReadPreference.secondary();
    		break;
    
    	case CONFIG_ADVANCED_READ_PREFERENCE_VALUE_SECONDARY_PREFERRED:
    		readPreference = ReadPreference.secondaryPreferred();
    		break;
    
    	case CONFIG_ADVANCED_READ_PREFERENCE_VALUE_NEAREST:
    		readPreference = ReadPreference.nearest();
    		break;
	    }
	    
	    // Store action timeout
	    actionTimeout = configuration.getIntFieldValue(CONFIG_ADVANCED_ACTION_TIMEOUT);

	    // Store the DB object
	    mongoDB = mongoClient.getDB(configuration.getFieldValue(CONFIG_DATABASE));

	    // Store the DB collection name - this is what the datasource code will use
	    mongoCollection = mongoDB.getCollection(configuration.getFieldValue(CONFIG_COLLECTION));
    	
	    // Store the filter key to use in the datastore to construct the MongoDB query
	    filterKey = CONFIG_FILTER;
	    
    	log.debug("---[ Configuration complete ]------");
    }
}
