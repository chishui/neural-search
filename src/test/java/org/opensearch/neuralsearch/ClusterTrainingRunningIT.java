/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import java.io.IOException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

public class ClusterTrainingRunningIT extends OpenSearchSecureRestTestCase {

    private static final int TEST_THREAD_COUNT = 8;
    private static final int MAX_REASONABLE_THREADS = 10;
    private static final int INVALID_THREAD_COUNT = -5;
    private static final String THREAD_QTY_SETTING_KEY = "neural.sparse.algo_param.index_thread_qty";
    private static final String BAD_REQUEST_ERROR = "400";
    private static final String BAD_REQUEST_TEXT = "Bad Request";
    private static final String CLUSTER_SETTINGS_ENDPOINT = "/_cluster/settings";
    private static final String THREAD_POOL_STATS_ENDPOINT = "/_nodes/stats/thread_pool";
    private static final String INCLUDE_DEFAULTS_PARAM = "?include_defaults=true";

    private Request createUpdateSettingRequest(int threadCount) {
        Request request = new Request("PUT", CLUSTER_SETTINGS_ENDPOINT);
        request.setJsonEntity(String.format("{\"transient\":{\"" + THREAD_QTY_SETTING_KEY + "\":%d}}", threadCount));
        return request;
    }

    private Request createResetSettingRequest() {
        Request request = new Request("PUT", CLUSTER_SETTINGS_ENDPOINT);
        request.setJsonEntity("{\"transient\":{\"" + THREAD_QTY_SETTING_KEY + "\":null}}");
        return request;
    }

    public void testThreadPoolSettingUpdate() throws IOException {
        Request updateRequest = createUpdateSettingRequest(TEST_THREAD_COUNT);
        Response updateResponse = client().performRequest(updateRequest);
        assertOK(updateResponse);
    }

    public void testThreadPoolStats() throws IOException {
        Request request = new Request("GET", THREAD_POOL_STATS_ENDPOINT);
        Response response = client().performRequest(request);
        assertOK(response);
    }

    public void testDefaultThreadPoolSetting() throws IOException {
        Request getRequest = new Request("GET", CLUSTER_SETTINGS_ENDPOINT + INCLUDE_DEFAULTS_PARAM);
        Response getResponse = client().performRequest(getRequest);
        assertOK(getResponse);
    }

    public void testThreadPoolSettingValidation() throws IOException {
        Request invalidUpdateRequest = createUpdateSettingRequest(INVALID_THREAD_COUNT);

        try {
            Response response = client().performRequest(invalidUpdateRequest);
            fail("Should have thrown exception for invalid thread count");
        } catch (Exception e) {
            assertTrue(
                "Exception should be related to invalid setting value",
                e.getMessage().contains(BAD_REQUEST_ERROR) || e.getMessage().contains(BAD_REQUEST_TEXT)
            );
        }
    }

    public void testThreadPoolSettingBoundaries() throws IOException {
        Request updateRequest = createUpdateSettingRequest(MAX_REASONABLE_THREADS);
        Response updateResponse = client().performRequest(updateRequest);
        assertOK(updateResponse);
    }

    public void testResetToDefaultSetting() throws IOException {
        Request resetRequest = createResetSettingRequest();
        Response resetResponse = client().performRequest(resetRequest);
        assertOK(resetResponse);
    }
}
