/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.SegmentInfo;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class CodecUtils {
    private static String buildIndexFilePrefix(String segmentName) {
        return String.format("%s_", segmentName);
    }

    private static String buildIndexFileSuffix(String fieldName, String extension) {
        return String.format("_%s%s", fieldName, extension);
    }

    /**
     * Builds a native index file name in the format: {@code <segmentName>_<version>_<fieldName><extension>}.
     *
     * @param segmentName        the Lucene segment name
     * @param latestBuildVersion the native engine version
     * @param fieldName          the sparse vector field name
     * @param extension          the engine file extension
     * @return the constructed file name
     */
    public static String buildIndexFileName(String segmentName, String latestBuildVersion, String fieldName, String extension) {
        return String.format("%s%s%s", buildIndexFilePrefix(segmentName), latestBuildVersion, buildIndexFileSuffix(fieldName, extension));
    }

    /**
     * Returns engine files for a field from the segment, sorted by name length.
     * Appends {@link SparseConstants#COMPOUND_EXTENSION} to the extension when compound file is used.
     *
     * @param extension   the base engine file extension
     * @param fieldName   the sparse vector field name
     * @param segmentInfo the segment to search
     * @return matching engine file names sorted by length
     */
    public static List<String> getEngineFiles(String extension, String fieldName, SegmentInfo segmentInfo) {
        /*
         * In case of compound file, extension would be <engine-extension> + c otherwise <engine-extension>
         */
        String engineExtension = segmentInfo.getUseCompoundFile() ? extension + SparseConstants.COMPOUND_EXTENSION : extension;
        final String engineSuffix = fieldName.isEmpty() ? engineExtension : "_" + fieldName + engineExtension;
        List<String> engineFiles = segmentInfo.files()
            .stream()
            .filter(fileName -> fileName.endsWith(engineSuffix))
            .sorted(Comparator.comparingInt(String::length))
            .collect(Collectors.toList());
        return engineFiles;
    }
}
