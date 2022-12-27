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

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;



public class ExecuteDynamoDBTest {

    private TestRunner testRunner;

	/**
	 * Descomentar para testar localmente
	 */
  @Before
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(ExecuteDynamoDB.class);

       	final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
      
    	testRunner.addControllerService("awsCredentialsProvider", serviceImpl);
    	testRunner.enableControllerService(serviceImpl);
    }
    
    @Test @Ignore
    public void testGETItemExistente() throws UnsupportedEncodingException, InitializationException {
    	
    	testRunner.setProperty(ExecuteDynamoDB.HASH_KEY, "cpf");
    	testRunner.setProperty(ExecuteDynamoDB.HASH_VALUE, "41259144860");
    	 //testRunner.setProperty(ExecuteDynamoDB.RANGE_KEY, "ProcessGroup");
		 testRunner.setProperty(ExecuteDynamoDB.UPDATE_ITEM_JSON, "{ \"name\": \"GUSTAVO BIUM DONADON\", \"gestorNome\": \"ROGERIO LUCIO DA SILVA\" }");
    	 //testRunner.setProperty(ExecuteDynamoDB.RANGE_VALUE, "cadastroPropriedades_cadastroProdutos");

    	 //testRunner.setProperty(ExecuteDynamoDB.FILTER_EXPRESSION, "#field = :value");
    	//testRunner.setProperty(ExecuteDynamoDB.FILTER_ATTRIBUTES, "{ \":value\": \"cadastroPropriedades_ad1e3c53-135b-4f19-abcd-8bd715d8bdbe\" }");
    	 //testRunner.setProperty(ExecuteDynamoDB.FILTER_NAMES, "{ \"#field\": \"LastEvaluatedKey\" }");

    	testRunner.setProperty(ExecuteDynamoDB.TABLE_NAME, "colaboradores");
    	testRunner.setProperty(ExecuteDynamoDB.FUNCTION, ExecuteDynamoDB.PUT_ITEM);
    	// testRunner.setProperty(ExecuteDynamoDB.TABLE_INDEX_NAME, "table");
    	//testRunner.setProperty(ExecuteDynamoDB.LAST_EVALUATED_KEY, "{ \"rangeKeyValue\": \"cadastroPropriedades_cadastroProdutos\", \"hashKeyValue\": \"cadastroPropriedades_bfe7b522-5abd-4c7d-95bf-2160bb4e2988\" }");
    	// testRunner.setProperty(ExecuteDynamoDB.LAST_EVALUATED_KEY, "null");
    	testRunner.setProperty(ExecuteDynamoDB.AWS_CREDENTIALS, "awsCredentialsProvider");
    	testRunner.setProperty(ExecuteDynamoDB.REGION, "us-east-1");
    	
    	testRunner.enqueue("testGETItemExistente");
		testRunner.run();

		List<MockFlowFile> ff = testRunner.getFlowFilesForRelationship(ExecuteDynamoDB.SUCCESS);
		// System.out.println(new String(ff.get(0).toByteArray(), "UTF-8") + "\n");
    }

}