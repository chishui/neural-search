/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import java.io.IOException;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

public class ClusterTrainingRunningIT extends OpenSearchSecureRestTestCase {

    public void testThreadPoolSettingUpdate() throws IOException, ParseException {
        Request updateRequest = new Request("PUT", "/_cluster/settings");
        updateRequest.setJsonEntity("""
            {
                "transient": {
                    "sparse.algo_param.index_thread_qty": 8
                }
            }
            """);
        Response updateResponse = client().performRequest(updateRequest);
        assertOK(updateResponse);

        Request getRequest = new Request("GET", "/_cluster/settings?include_defaults=true");
        Response getResponse = client().performRequest(getRequest);
        assertOK(getResponse);
        System.out.println(EntityUtils.toString(getResponse.getEntity()));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertTrue(responseBody.contains("\"sparse\":{\"algo_param\":{\"index_thread_qty\":\"8\"}"));
    }

    public void testThreadPoolStats() throws IOException, ParseException {
        Request request = new Request("GET", "/_nodes/stats/thread_pool");
        Response response = client().performRequest(request);
        assertOK(response);

        String responseBody = EntityUtils.toString(response.getEntity());
        assertNotNull(responseBody);
        assertTrue(responseBody.contains("thread_pool"));
        // Check if cluster_training_thread_pool exists in the response
        assertTrue(responseBody.contains("cluster_training_thread_pool"));
    }

    public void testDefaultThreadPoolSetting() throws IOException, ParseException {
        // Test that the default setting is properly handled
        Request getRequest = new Request("GET", "/_cluster/settings?include_defaults=true");
        Response getResponse = client().performRequest(getRequest);
        assertOK(getResponse);

        String responseBody = EntityUtils.toString(getResponse.getEntity());
        // The default value should be -1, which means auto-calculated
        assertTrue(responseBody.contains("\"sparse\":{\"algo_param\":{\"index_thread_qty\""));
    }
}
