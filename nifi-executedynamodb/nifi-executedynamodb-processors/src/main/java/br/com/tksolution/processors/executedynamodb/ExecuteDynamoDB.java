/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package br.com.tksolution.processors.executedynamodb;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.xspec.QueryExpressionSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.utils.ImmutableMap;

@Tags({"dynamodb"})
@CapabilityDescription("This is a generic DynamoDB functionalities to save, update and get an item from dynamo. ")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="Item-Found", description="On function GetItem if an item doesn't exists set to otherwise sets to true")})
public class ExecuteDynamoDB extends AbstractProcessor {

	public static final String PUT_ITEM = "Update Item";
    public static final String GET_ITEM = "Get Item";
	public static final String QUERY_TABLE = "Query Table";
	public static final String SCAN_TABLE = "Scan Table";

    public static final PropertyDescriptor HASH_KEY = new PropertyDescriptor
            .Builder().name("HASH_KEY")
            .displayName("Hash key")
            .description("Ex: ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor HASH_VALUE = new PropertyDescriptor
            .Builder().name("HASH_VALUE")
            .displayName("Hash value")
            .description("Ex: ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor RANGE_KEY = new PropertyDescriptor
            .Builder().name("RANGE_KEY")
            .displayName("Range key")
            .description("")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor RANGE_VALUE = new PropertyDescriptor
            .Builder().name("RANGE_VALUE")
            .displayName("Range value")
            .description("")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name("TABLE_NAME")
            .displayName("Table name for put item on DynamoDB")
            .description("")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor FUNCTION = new PropertyDescriptor
            .Builder().name("FUNCTION")
            .displayName("Function to call")
            .description("Save, update or get an item. If you get an item the content of the flowfile will be modify")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(PUT_ITEM, GET_ITEM, QUERY_TABLE, SCAN_TABLE)
            .defaultValue(GET_ITEM)
            .build();

	public static final PropertyDescriptor TABLE_INDEX_NAME = new PropertyDescriptor
            .Builder().name("TABLE_INDEX_NAME")
            .displayName("Table index name")
            .description("If this attribute is set then it will be used to make a query request on table")
            .dependsOn(FUNCTION, QUERY_TABLE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

	public static final PropertyDescriptor FILTER_EXPRESSION = new PropertyDescriptor
            .Builder().name("FILTER_EXPRESSION")
            .displayName("Filter Expression")
            .description("Filter Expression with conditions, ex: \"field = :value\"")
            .dependsOn(FUNCTION, QUERY_TABLE, SCAN_TABLE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

	public static final PropertyDescriptor FILTER_ATTRIBUTES = new PropertyDescriptor
            .Builder().name("FILTER_ATTRIBUTES")
            .displayName("Filter Attributes")
            .description("Filter Attributes as JSON. ex: { \":val\": { \"S\": \"your_value\" } }")
            .dependsOn(FUNCTION, QUERY_TABLE, SCAN_TABLE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

	public static final PropertyDescriptor FILTER_NAMES = new PropertyDescriptor
            .Builder().name("FILTER_NAMES")
            .displayName("Filter Key Names")
            .description("Filter Key Names as JSON. ex: { \"#val\": { \"S\": \"your_value\" } }")
            .dependsOn(FUNCTION, QUERY_TABLE, SCAN_TABLE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

	public static final PropertyDescriptor LAST_EVALUATED_KEY = new PropertyDescriptor
            .Builder().name("LAST_EVALUATED_KEY")
            .displayName("Last Evaluated Key")
            .description("Optional, if you want to continue a scan or query request")
            .dependsOn(FUNCTION, QUERY_TABLE, SCAN_TABLE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

	public static final PropertyDescriptor UPDATE_ITEM_JSON = new PropertyDescriptor
            .Builder().name("UPDATE_ITEM_JSON")
            .displayName("Update Item JSON")
            .description("A JSON with the new keys and values to update")
            .dependsOn(FUNCTION, PUT_ITEM)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
        
    public static final PropertyDescriptor AWS_CREDENTIALS = org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
    public static final PropertyDescriptor REGION = org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.REGION;

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();

	public static final Relationship REL_SPLIT = new Relationship.Builder()
		.name("split")
		.description("All segments of the original FlowFile will be routed to this relationship")
		.build();

	public static final Relationship NOT_FOUND = new Relationship.Builder()
		.name("not found")
		.description("When item is not found flowfile will be routed to this relationship")
		.build();
    

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
		descriptors.add(FUNCTION);
		descriptors.add(TABLE_NAME);
		descriptors.add(TABLE_INDEX_NAME);
        descriptors.add(HASH_KEY);
        descriptors.add(RANGE_KEY);
        descriptors.add(HASH_VALUE);
        descriptors.add(RANGE_VALUE);
        descriptors.add(AWS_CREDENTIALS);
        descriptors.add(REGION);
        descriptors.add(FILTER_EXPRESSION);
        descriptors.add(FILTER_ATTRIBUTES);
        descriptors.add(FILTER_NAMES);
        descriptors.add(LAST_EVALUATED_KEY);
        descriptors.add(UPDATE_ITEM_JSON);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships.add(NOT_FOUND);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }
    
    private ObjectMapper mapper = new ObjectMapper();
        
    boolean itemFound;

    /* (non-Javadoc)
     * @see org.apache.nifi.processor.AbstractProcessor#onTrigger(org.apache.nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		// System.out.println("Starting processor....");
		itemFound = true;
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

        try {

        	Regions region = Regions.fromName(context.getProperty(REGION).toString());//Regions.valueOf(context.getProperty(REGION).toString());
        	
        	AWSCredentialsProviderService awsCredentialsProviderService =
                    context.getProperty(AWS_CREDENTIALS).asControllerService(AWSCredentialsProviderService.class);       	        	
        			
        	String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        	String function = context.getProperty(FUNCTION).getValue();
        	String hashKey = context.getProperty(HASH_KEY).evaluateAttributeExpressions(flowFile).getValue();
        	String hashValue = context.getProperty(HASH_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        	String rangeKey = context.getProperty(RANGE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        	String rangeValue = context.getProperty(RANGE_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        	// String attributeKey = context.getProperty(ATTRIBUTES_KEY).evaluateAttributeExpressions(flowFile).getValue();
        	// String attributeValue = context.getProperty(ATTRIBUTES_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        	String tableIndexName = context.getProperty(TABLE_INDEX_NAME).evaluateAttributeExpressions(flowFile).getValue();
        	String QueryFilterExpression = context.getProperty(FILTER_EXPRESSION).evaluateAttributeExpressions(flowFile).getValue();
        	String QueryFilterAttributes = context.getProperty(FILTER_ATTRIBUTES).evaluateAttributeExpressions(flowFile).getValue();
        	String filterNames = context.getProperty(FILTER_NAMES).evaluateAttributeExpressions(flowFile).getValue();
        	String lastEvaluatedKey = context.getProperty(LAST_EVALUATED_KEY).evaluateAttributeExpressions(flowFile).getValue();
        	String updateItemJSON = context.getProperty(UPDATE_ITEM_JSON).evaluateAttributeExpressions(flowFile).getValue();    		
    		ExecuteDynamoDBDocumentClient client = ExecuteDynamoDBDocumentClient.getInstance(awsCredentialsProviderService.getCredentialsProvider(), region);
    	
    		AmazonDynamoDB ddb = client.getDocumentClient();
        	
        	DynamoDB dynamoDB = new DynamoDB(ddb);
    		
			Table table = dynamoDB.getTable(tableName);
    		
        	flowFile = session.write(flowFile, new StreamCallback() {
	   			@Override
	   			public void process(InputStream in, OutputStream out) throws IOException {
   					final InputStream bufferedIn = new BufferedInputStream(in);
   					
   		    		switch (function) {

   					case PUT_ITEM:
   						String updateItemOutput = updateDynamoDBItem(hashKey, hashValue, rangeKey, rangeValue, updateItemJSON, table);
						IOUtils.write(updateItemOutput, out, Charset.defaultCharset());
   						break;
   					case GET_ITEM:
   							itemFound = true;
   							String output = getDynamoDBItem(hashKey, hashValue, rangeKey, rangeValue, table);
   							if(!output.equalsIgnoreCase("-1")) {
   								IOUtils.write(output, out, Charset.defaultCharset());
   							} else {
   								itemFound = false;
   							}
   						break;
   					case SCAN_TABLE:
   						String output3 = ScanTable(table, QueryFilterExpression, QueryFilterAttributes, filterNames, tableIndexName, hashKey, hashValue, rangeKey, rangeValue, lastEvaluatedKey);
   						IOUtils.write(output3, out, Charset.defaultCharset());
   						break;

   					case QUERY_TABLE:
					   	String output4 = QueryTable(table, QueryFilterExpression, QueryFilterAttributes, filterNames, tableIndexName, hashKey, hashValue, rangeKey, rangeValue);
   						IOUtils.write(output4, out, Charset.defaultCharset());
   						break;

   					default:
   						break;
   					}	   					
	   			}
	       	});
        	
			if(itemFound == false) {
        		session.putAttribute(flowFile, "Item-Found", "false");
				session.transfer(flowFile, NOT_FOUND);
        	} else if (itemFound == true) {
        		session.putAttribute(flowFile, "Item-Found", "true");
				session.transfer(flowFile, SUCCESS);
        	}
        	
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			getLogger().error("erro: " + e.getMessage() + "erro2: " + e.getLocalizedMessage() + "erro3: " + pw.toString() + sw.toString());
			
			session.putAttribute(flowFile, "Exception", "erro: " + e.getMessage() + "erro2: " + e.getLocalizedMessage() + "erro3: " + pw.toString() + sw.toString());
			session.transfer(flowFile, FAILURE);
		}
    }	

	protected UpdateItemSpec prepareUpdateSpec(String hashKey, String hashValue, String rangeKey, String rangeValue, String updateItemJSON) {
		UpdateItemSpec updateItemSpec = new UpdateItemSpec();
		boolean useRangeKey = (rangeKey != null && rangeValue != null) ? true : false;
		JSONObject newItemJSON = new JSONObject(updateItemJSON);



		if (useRangeKey) {
			updateItemSpec.withPrimaryKey(hashKey, hashValue, rangeKey, rangeValue);
		} else {
			updateItemSpec.withPrimaryKey(hashKey, hashValue);
		}

		for (String key : newItemJSON.keySet()) {
			updateItemSpec.addAttributeUpdate(new AttributeUpdate(key).put(newItemJSON.get(key)));
		}

		updateItemSpec.withReturnValues(ReturnValue.ALL_NEW);

		return updateItemSpec;
	}
	
	protected String updateDynamoDBItem(String hashKey, String hashValue, String rangeKey, String rangeValue, String updateItemJSON, Table table) {
		UpdateItemSpec updateItemSpec = prepareUpdateSpec(hashKey, hashValue, rangeKey, rangeValue, updateItemJSON);
		String output = table.updateItem(updateItemSpec).getItem().toJSON();

		return output;
	}
	
	public static boolean isNumeric(String strNum) {
	    if (strNum == null) {
	        return false;
	    }
	    try {
	        double d = Double.parseDouble(strNum);
	    } catch (NumberFormatException nfe) {
	        return false;
	    }
	    return true;
	}
	
	protected String getDynamoDBItem(String hashKey, String hashValue, String rangeKey, String rangeValue, Table table) {
		Item item;
		if(rangeKey == null) {
			item = table.getItem(hashKey, hashValue);
		}else {
			item = table.getItem(hashKey, hashValue, rangeKey, rangeValue);
		}
		
		if(item == null) {
			return  "-1";
		}else {
			// System.out.println(item.toJSON());
			return item.toJSON();	
		}
	}

	public QuerySpec prepareQuerySpec(Table table, String ScanFilterExpression, String SFAttributes, String FilterNames, String IndexName, String hashKey, String hashValue, String rangeKey, String rangeValue) {
		QuerySpec spec;
		boolean useScanFilterAttributes = (SFAttributes == null) ? false : true;
		boolean useScanFilterExpression = (ScanFilterExpression == null) ? false : true;
		boolean useFilterNames = (FilterNames == null) ? false : true;
		Integer resultSize = 2000;

		ValueMap ScanFilterAttributes = new ValueMap();
		NameMap ScanFilterNames = new NameMap();

		// Adicionando as chaves do JSON para o ValueMap
		if (useScanFilterAttributes) {
			JSONObject ScanFilterAttributesJSON = new JSONObject(SFAttributes);
			for (Object key : ScanFilterAttributesJSON.keySet()) {
				ScanFilterAttributes.put(key.toString(), ScanFilterAttributesJSON.get(key.toString()));
			}
		}

		if (useFilterNames) {
			JSONObject filterNamesJSON = new JSONObject(FilterNames);
			for (Object key : filterNamesJSON.keySet()) {
				ScanFilterNames.put(key.toString(), filterNamesJSON.get(key.toString()).toString());
			}
		}

		spec = new QuerySpec()
		.withHashKey(new KeyAttribute(hashKey, hashValue));
		
		if (useScanFilterExpression) {
				spec.withFilterExpression(ScanFilterExpression);

			if (useScanFilterAttributes) {
				spec.withValueMap(ScanFilterAttributes);
			}

			if (useFilterNames) {	
				spec.withNameMap(ScanFilterNames);
			}
		}

		spec.withMaxResultSize(resultSize);

		return spec;
	}

	protected String QueryTable(Table table, String QueryFilterExpression, String SFAttributes, String FilterNames, String IndexName, String hashKey, String hashValue, String rangeKey, String rangeValue) {	
		ItemCollection<QueryOutcome> items;
		QuerySpec spec = prepareQuerySpec(table, QueryFilterExpression, SFAttributes, FilterNames, IndexName, hashKey, hashValue, rangeKey, rangeValue);

		if (IndexName != null) {
			// System.out.println("IndexName is " + IndexName);
			Index tableIndex = table.getIndex(IndexName);
			items = tableIndex.query(spec);
		} else {
			items = table.query(spec);
		}

		JSONObject output = new JSONObject();
		List<JSONObject> documents = new ArrayList<JSONObject>();
		
		for (Item item : items) {
			documents.add(new JSONObject(item.toJSON()));
		}

		output.put("documents", documents);

		if (items.getLastLowLevelResult().getQueryResult().getLastEvaluatedKey() != null) {
			output.put("LastEvaluatedKey", new JSONObject(ItemUtils.toItem(items.getLastLowLevelResult().getQueryResult().getLastEvaluatedKey()).toJSON()));
		}

		// System.out.println(output.toString());

		return output.toString();
	}

	public ScanSpec prepareScanSpec(Table table, String ScanFilterExpression, String SFAttributes, String FilterNames, String IndexName, String hashKey, String hashValue, String rangeKey, String rangeValue, String lastEvaluatedKey) {
		lastEvaluatedKey = lastEvaluatedKey.toString();
		ScanSpec spec;
		boolean useScanFilterAttributes = (SFAttributes == null) ? false : true;
		boolean useScanFilterExpression = (ScanFilterExpression == null) ? false : true;
		boolean useFilterNames = (FilterNames == null) ? false : true;
		boolean useLastEvaluatedKey = (lastEvaluatedKey == null || lastEvaluatedKey.equalsIgnoreCase("null")) ? false : true;
		Integer resultSize = 2000;

		ValueMap ScanFilterAttributes = new ValueMap();
		NameMap ScanFilterNames = new NameMap();

		// Adicionando as chaves do JSON para o ValueMap
		if (useScanFilterAttributes) {
			JSONObject ScanFilterAttributesJSON = new JSONObject(SFAttributes);
			for (Object key : ScanFilterAttributesJSON.keySet()) {
				ScanFilterAttributes.put(key.toString(), ScanFilterAttributesJSON.get(key.toString()));
			}
		}

		if (useFilterNames) {
			JSONObject filterNamesJSON = new JSONObject(FilterNames);
			for (Object key : filterNamesJSON.keySet()) {
				ScanFilterNames.put(key.toString(), filterNamesJSON.get(key.toString()).toString());
			}
		}

		spec = new ScanSpec();
		
		if (useScanFilterExpression) {
				spec.withFilterExpression(ScanFilterExpression);

			if (useScanFilterAttributes) {
				spec.withValueMap(ScanFilterAttributes);
			}

			if (useFilterNames) {
				spec.withNameMap(ScanFilterNames);
			}
		}

		if (useLastEvaluatedKey) {
			JSONObject lastEvaluatedKeyJSON = new JSONObject(lastEvaluatedKey);
			spec.withExclusiveStartKey(hashKey, lastEvaluatedKeyJSON.get("hashKeyValue"), rangeKey, lastEvaluatedKeyJSON.get("rangeKeyValue"));
			// spec.withExclusiveStartKey(lastEvaluatedKeyMap);
		}

		spec.withMaxResultSize(resultSize);
		return spec;
	}

	public String ScanTable(Table table, String ScanFilterExpression, String SFAttributes, String FilterNames, String IndexName, String hashKey, String hashValue, String rangeKey, String rangeValue, String lastEvaluatedKey) {
		ItemCollection<ScanOutcome> items;
		ScanSpec spec = prepareScanSpec(table, ScanFilterExpression, SFAttributes, FilterNames, IndexName, hashKey, hashValue, rangeKey, rangeValue, lastEvaluatedKey);

		if (IndexName != null) {
			Index tableIndex = table.getIndex(IndexName);
			items = tableIndex.scan(spec);
		} else {
			items = table.scan(spec);
		}

		// HashMap<String, String> output = new HashMap<String, String>();
		JSONObject output = new JSONObject();
		List<JSONObject> documents = new ArrayList<JSONObject>();

		for (Item item : items) {
			documents.add(new JSONObject(item.toJSON()));
		}

		output.put("documents", documents);

		if (items.getLastLowLevelResult().getScanResult().getLastEvaluatedKey() != null) {
			output.put("LastEvaluatedKey", new JSONObject(ItemUtils.toItem(items.getLastLowLevelResult().getScanResult().getLastEvaluatedKey()).toJSON()));
		}

		//System.out.println(output);
		return output.toString();
	}

}