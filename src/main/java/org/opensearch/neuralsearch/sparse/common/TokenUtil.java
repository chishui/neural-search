/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.reflect.TypeToken;
import static org.opensearch.ml.common.utils.StringUtils.gson;

/**
 * Helper class with cluster representatives and assignments. Often used to getTopClusters from a query sketch.
 */
@Log4j2
public class TokenUtil {

    Map<String, Integer> tokenIdMapping = new HashMap<>(); // mapping from token to the id

    private static final String TOKENIZER_RESOURCE = "tokenizer.json";

    // Instance is created at class loading time
    private static volatile TokenUtil INSTANCE;

    private TokenUtil() {
        loadTokenIdMapping();
    }

    // lazy load
    public static TokenUtil getInstance() {
        if (INSTANCE == null) {
            synchronized (TokenUtil.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TokenUtil();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Loads token id mapping from a file in the temporary directory
     */
    private void loadTokenIdMapping() {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                String tempDir = System.getProperty("java.io.tmpdir");
                File file = new File(tempDir, TokenUtil.TOKENIZER_RESOURCE);

                if (!file.exists() || !file.canRead()) {
                    System.err.println("tokenizer file doesn't exist or isn't readable: " + file.getAbsolutePath());
                    return null;
                }

                try (FileInputStream fis = new FileInputStream(file)) {
                    InputStreamReader reader = new InputStreamReader(fis);

                    Type mapType = new TypeToken<Map<String, Object>>() {
                    }.getType();

                    Map<String, Object> tokenizerMap = gson.fromJson(reader, mapType);
                    tokenizerMap = (Map<String, Object>) tokenizerMap.get("model");
                    Map<String, Double> doubleMap = (Map<String, Double>) tokenizerMap.get("vocab");
                    for (Map.Entry<String, Double> entry : doubleMap.entrySet()) {
                        this.tokenIdMapping.put(entry.getKey(), entry.getValue().intValue());
                    }
                    System.out.println("Successfully loaded token id mapping file");
                } catch (IOException e) {
                    System.err.println("Error reading token id mapping file: " + e.getMessage());
                }
                return null;
            });
        } catch (PrivilegedActionException e) {
            System.err.println("Security error while loading token id mapping file: " + e.getException());
        }
    }

    public Integer getTokenId(String token) {
        if (tokenIdMapping.containsKey(token)) {
            return tokenIdMapping.get(token);
        }
        throw new RuntimeException("Token does not exist");
    }

}
