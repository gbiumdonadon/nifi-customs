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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.apache.nifi.reporting.InitializationException;


public class ExecuteAthenaTest {

    private TestRunner testRunner;

    @Before
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(ExecuteAthena.class);
    }

    @Test
    public void testProcessor() {
        testRunner.setProperty(ExecuteAthena.ATHENA_DATABASE, "dumper_database");
        testRunner.setProperty(ExecuteAthena.ATHENA_TABLE, "dumper_table");
        testRunner.setProperty(ExecuteAthena.ATHENA_QUERY, "SELECT * FROM dumper_table WHERE process = 'Exception' limit 10;");
        testRunner.setProperty(ExecuteAthena.OUTPUT_LOCATION, "s3://tks-processtracker-dumper-output");
        // testRunner.setProperty(ExecuteAthena.NEXT_TOKEN, "");

        testRunner.enqueue("x");
		testRunner.run();

    }

}
