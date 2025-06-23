/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ALGO_TRIGGER_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ALPHA_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.LAMBDA_FIELD;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Seismic implements SparseAlgorithm {
    public static final Seismic INSTANCE;

    static {
        INSTANCE = new Seismic();
    }

    @Override
    public ValidationException validateMethod(SparseMethodContext sparseMethodContext) {
        String algoName = sparseMethodContext.getMethodComponentContext().getName();
        ValidationException validationException = null;
        final List<String> errorMessages = new ArrayList<>();
        Map<String, Object> parameters = new HashMap<>(sparseMethodContext.getMethodComponentContext().getParameters());
        if (parameters.containsKey(ALPHA_FIELD)) {
            float alpha = ((Number) parameters.get(ALPHA_FIELD)).floatValue();
            if (alpha <= 0 || alpha > 1) {
                errorMessages.add("summary prune ratio should be in (0, 1]");
            }
            parameters.remove(ALPHA_FIELD);
        }
        if (parameters.containsKey(LAMBDA_FIELD)) {
            Integer lambda = (Integer) parameters.get(LAMBDA_FIELD);
            if (lambda <= 0) {
                errorMessages.add("n_postings should be a positive integer");
            }
            parameters.remove(LAMBDA_FIELD);
        }
        if (parameters.containsKey(CLUSTER_RATIO_FIELD)) {
            float clusterRatio = ((Number) parameters.get(CLUSTER_RATIO_FIELD)).floatValue();
            if (clusterRatio <= 0 || clusterRatio >= 1) {
                errorMessages.add("cluster ratio should be in (0, 1)");
            }
            parameters.remove(CLUSTER_RATIO_FIELD);
        }
        if (parameters.containsKey(ALGO_TRIGGER_THRESHOLD_FIELD)) {
            Integer algoTriggerThreshold = (Integer) parameters.get(ALGO_TRIGGER_THRESHOLD_FIELD);
            if (algoTriggerThreshold < 0) {
                errorMessages.add("algo trigger doc count should be a non-negative integer");
            }
            parameters.remove(ALGO_TRIGGER_THRESHOLD_FIELD);
        }
        for (String key : parameters.keySet()) {
            errorMessages.add(String.format(Locale.ROOT, "Unknown parameter '%s' found", key));
        }
        if (!errorMessages.isEmpty()) {
            validationException = new ValidationException();
            validationException.addValidationErrors(errorMessages);
        }
        return validationException;
    }
}
