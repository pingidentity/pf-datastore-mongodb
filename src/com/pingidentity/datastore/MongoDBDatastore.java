package com.pingidentity.datastore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.sourceid.saml20.adapter.conf.Configuration;
import org.sourceid.saml20.adapter.conf.SimpleFieldList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoTimeoutException;
import com.pingidentity.sources.CustomDataSourceDriver;
import com.pingidentity.sources.SourceDescriptor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class MongoDBDatastore implements CustomDataSourceDriver
{
    // instantiate and obtain the config object
    private MongoDBDatastoreConfiguration config = new MongoDBDatastoreConfiguration();

    // initialise a logger
    private final Log log = LogFactory.getLog(this.getClass());

    
    public MongoDBDatastore() { }

    @Override
    public boolean testConnection()
    {
    	log.debug("---[ Testing connectivity to MongoDB ]------");
    	
    	try {

    		DBCursor cursor = config.mongoCollection.find().limit(1).maxTime(config.actionTimeout, MILLISECONDS);;

        	if(cursor.hasNext()) {
        		return true;
        	}
        	
    	} catch(MongoTimeoutException ex) {
    		log.debug("Caught MTE: " + ex.getMessage());
    		return false;
    	} catch(Exception e) {
    		log.debug("Caught exception: " + e.getMessage());
    		return false;
    	}
    	
    	return false;
    }

    @Override
    public Map<String, Object> retrieveValues(Collection<String> attributeNamesToFill, SimpleFieldList filterConfiguration)
    {
    	log.debug("---[ Retrieving Values ]------");
    	Map<String, Object> returnMap = new HashMap<String, Object>();
    	
    	BasicDBObject mongoDBQuery = buildMongoQueryFromFilter(filterConfiguration);
    	
    	try {
        	DBCursor cursor = config.mongoCollection.find(mongoDBQuery).limit(1).maxTime(config.actionTimeout, MILLISECONDS);

        	if(cursor.hasNext()) {
        		DBObject entry = cursor.next();
        		
        		for(String attribute : attributeNamesToFill) {
        			log.debug("Checking for attribute: " + attribute);
        			if (entry.containsField(attribute)) {
        				log.debug(" - returning value: " + entry.get(attribute));
        				returnMap.put(attribute, entry.get(attribute));
        			} else {
        				log.debug(" - returning value: null");
        				returnMap.put(attribute, null);
        			}
        		}

        	} else {
        		
        		log.info("No object found");
        	}
    	} catch(MongoTimeoutException ex) {
    		log.error("ERROR: Timeout occurred - " + ex.getMessage());
    	}

		return returnMap;
    }

	@Override
    public List<String> getAvailableFields()
    {
    	log.debug("---[ Retrieving Available Fields ]------");
    	List<String> availableFields = new ArrayList<String>();

    	try {
        	DBCursor cursor = config.mongoCollection.find().limit(1).maxTime(config.actionTimeout, MILLISECONDS);;

        	if(cursor.hasNext()) {
        		DBObject schema = cursor.next();
        	
        		for (String k : schema.keySet()) {
        			availableFields.add(k);
        		}
        	}

    	} catch(MongoTimeoutException ex) {
    		log.error("ERROR: Timeout occurred - " + ex.getMessage());
    	}

    	return sortList(availableFields);
    }
	
	/**
	 * The getSourceDescriptor method returns the configuration details.
	 */
	@Override
	public SourceDescriptor getSourceDescriptor() {
		return config.getSourceDescriptor(this);
	}

	/**
	 * The configure method sets the configuration details.
	 */
	@Override
	public void configure(Configuration configuration) {
		config.configure(configuration);
	}
	
	
	private BasicDBObject buildMongoQueryFromFilter(SimpleFieldList filter) {

    	log.debug("---[ Building the MongoDB query ]------");
    	// lets make this very basic for now: attribute = value

    	BasicDBObject mongoQuery = null;
    	String rawFilter = filter.getFieldValue(config.filterKey);
    	
    	log.debug(" - Raw filter: " + rawFilter);

    	if (rawFilter.contains("=")) {
        	String[] filterComponents = rawFilter.split("=");
        	String attribute = filterComponents[0].trim();
        	String value = filterComponents[1].trim();
        	mongoQuery = new BasicDBObject(attribute, value);
    	} else {
    		log.error("Invalid filter: " + rawFilter);
    	}
    	
    	return mongoQuery;
	}
	
    private static <T extends Comparable<? super T>> List<T> sortList(Collection<T> c) {
        List<T> list = new ArrayList<T>(c);
        java.util.Collections.sort(list);
        return list;
      }
	
}

