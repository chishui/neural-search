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

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ALGO_TRIGGER_DOC_COUNT_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;

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
        if (parameters.containsKey(SUMMARY_PRUNE_RATIO_FIELD)) {
            try {
                Object value = parameters.get(SUMMARY_PRUNE_RATIO_FIELD);
                float summaryPruneRatio;
                if (value instanceof Float) {
                    summaryPruneRatio = ((Number) value).floatValue();
                } else if (value instanceof String) {
                    summaryPruneRatio = Float.parseFloat((String) value);
                } else {
                    throw new ClassCastException();
                }
                if (summaryPruneRatio <= 0 || summaryPruneRatio > 1) {
                    errorMessages.add("summary prune ratio should be in (0, 1]");
                }
            } catch (ClassCastException | NumberFormatException | NullPointerException e) {
                errorMessages.add("summary prune ratio should be a valid number");
            }
            parameters.remove(SUMMARY_PRUNE_RATIO_FIELD);
        }
        if (parameters.containsKey(N_POSTINGS_FIELD)) {
            try {
                Object value = parameters.get(N_POSTINGS_FIELD);
                Integer nPostings;
                if (value instanceof Integer) {
                    nPostings = (Integer) value;
                } else if (value instanceof String) {
                    nPostings = Integer.parseInt((String) value);
                } else {
                    throw new ClassCastException();
                }
                if (nPostings <= 0) {
                    errorMessages.add("n_postings should be a positive integer");
                }
            } catch (ClassCastException | NumberFormatException | NullPointerException e) {
                errorMessages.add("n_postings should be a valid integer");
            }
            parameters.remove(N_POSTINGS_FIELD);
        }
        if (parameters.containsKey(CLUSTER_RATIO_FIELD)) {
            try {
                Object value = parameters.get(CLUSTER_RATIO_FIELD);
                float clusterRatio;
                if (value instanceof Float) {
                    clusterRatio = ((Number) value).floatValue();
                } else if (value instanceof String) {
                    clusterRatio = Float.parseFloat((String) value);
                } else {
                    throw new ClassCastException();
                }
                if (clusterRatio <= 0 || clusterRatio >= 1) {
                    errorMessages.add("cluster ratio should be in (0, 1)");
                }
            } catch (ClassCastException | NumberFormatException | NullPointerException e) {
                errorMessages.add("cluster ratio should be a valid number");
            }
            parameters.remove(CLUSTER_RATIO_FIELD);
        }
        if (parameters.containsKey(ALGO_TRIGGER_DOC_COUNT_FIELD)) {
            try {
                Object value = parameters.get(ALGO_TRIGGER_DOC_COUNT_FIELD);
                Integer algoTriggerThreshold;
                if (value instanceof Integer) {
                    algoTriggerThreshold = (Integer) value;
                } else if (value instanceof String) {
                    algoTriggerThreshold = Integer.parseInt((String) value);
                } else {
                    throw new ClassCastException();
                }
                if (algoTriggerThreshold < 0) {
                    errorMessages.add("algo trigger doc count should be a non-negative integer");
                }
            } catch (ClassCastException | NumberFormatException | NullPointerException e) {
                errorMessages.add("algo trigger doc count should be a valid integer");
            }
            parameters.remove(ALGO_TRIGGER_DOC_COUNT_FIELD);
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
