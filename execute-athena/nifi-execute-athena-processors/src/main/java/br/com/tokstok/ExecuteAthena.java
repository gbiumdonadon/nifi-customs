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
package br.com.tokstok;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.json.JSONObject;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"Athena", "AWS", "Tokstok"})
@CapabilityDescription("Execute Athena Query")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ExecuteAthena extends AbstractProcessor {

    public static final PropertyDescriptor ATHENA_DATABASE = new PropertyDescriptor
            .Builder().name("Athena Database")
            .displayName("Athena Database")
            .description("Database to query")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATHENA_TABLE = new PropertyDescriptor
            .Builder().name("Athena Table")
            .displayName("Athena Table")
            .description("Table to query")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_LOCATION = new PropertyDescriptor
            .Builder().name("S3 Output Location")
            .displayName("S3 Output Location")
            .description("Output location for query results")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATHENA_QUERY = new PropertyDescriptor
            .Builder().name("Athena Query")
            .displayName("Athena Query")
            .description("Query to execute")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor NEXT_TOKEN = new PropertyDescriptor
            .Builder().name("Next Token")
            .displayName("Next Token")
            .description("Next Token for pagination")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(ATHENA_DATABASE);
        descriptors.add(ATHENA_TABLE);
        descriptors.add(OUTPUT_LOCATION);
        descriptors.add(ATHENA_QUERY);
        descriptors.add(NEXT_TOKEN);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try {
            AthenaClient athenaClient = AthenaClient.builder()
            .build();

            String queryExecutionId = submitAthenaQuery(athenaClient, context.getProperty(ATHENA_DATABASE).getValue(), context.getProperty(OUTPUT_LOCATION).getValue(), context.getProperty(ATHENA_QUERY).getValue());
            waitForQueryToComplete(athenaClient, queryExecutionId);
            String flowfileContent = processResultRows(athenaClient, queryExecutionId, context.getProperty(NEXT_TOKEN).getValue());
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    IOUtils.write(flowfileContent, out, Charset.defaultCharset());
                }
            });
            
            athenaClient.close();
            session.transfer(flowFile, SUCCESS);
            
        } catch (Exception e) {
            session.putAttribute(flowFile, "Execute Athena Exception", e.getMessage());
            session.transfer(flowFile, FAILURE);
            // System.out.println(e.getMessage());
        }      

    }

    public static String submitAthenaQuery(AthenaClient athenaClient, String database, String outputLocation, String athenaQuery) {
        try {
            // QueryExecutionContext para especificar em qual database estaremos trabalhando.
            QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                .database(database)
                .build();

            // ResultConfiguration para especificar em qual bucket o resultado da query irá.
            ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                .outputLocation(outputLocation)
                .build();

            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(athenaQuery)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration)
                .build();

            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            return startQueryExecutionResponse.queryExecutionId();

        } catch (AthenaException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return "";
    }

    public static void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
            .queryExecutionId(queryExecutionId)
            .build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("The Amazon Athena query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Tempo para aguardar antes de fazer uma nova tentativa.
                Thread.sleep(1000);
            }
        }
    }

    public static String processResultRows(AthenaClient athenaClient, String queryExecutionId, String nextToken) {
        try {

            JSONObject result = new JSONObject();
            GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                .queryExecutionId(queryExecutionId)
                .maxResults(1000)
                .nextToken(nextToken)
                .build();

            GetQueryResultsResponse getQueryResultsResponse = athenaClient.getQueryResults(getQueryResultsRequest);
            String paginationNextToken = getQueryResultsResponse.nextToken();   

            List<ColumnInfo> columnInfoList = getQueryResultsResponse.resultSet().resultSetMetadata().columnInfo();
            List<Row> results = getQueryResultsResponse.resultSet().rows();
            ArrayList<JSONObject> rows = processRow(results, columnInfoList);
            result.put("rows", rows);
            result.put("nextToken", paginationNextToken);
            return result.toString();

        } catch (AthenaException e) {
           e.printStackTrace();
           System.exit(1);
       }
        return "";
    }

    private static ArrayList<JSONObject> processRow(List<Row> row, List<ColumnInfo> columnInfoList) {
        ArrayList<JSONObject> resultJArray = new ArrayList<JSONObject>();

        for (Row myRow : row) {
            List<Datum> allData = myRow.data();
            JSONObject result = new JSONObject();
            for (Datum data : allData) {
                result.put(columnInfoList.get(allData.indexOf(data)).name(), data.varCharValue());
            }
            resultJArray.add(result);
        }

        return resultJArray;
    }
}
