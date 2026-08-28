/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@AllArgsConstructor
public class SparseCompoundFormat extends CompoundFormat {
    private final CompoundFormat delegate;

    @Override
    public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si) throws IOException {
        return new SparseCompoundDirectory(delegate.getCompoundReader(dir, si), dir);
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        String engineExtension = SparseEngine.NATIVE.extension();
        Set<String> engineFiles = si.files().stream().filter(file -> file.endsWith(engineExtension)).collect(Collectors.toSet());

        Set<String> segmentFiles = new HashSet<>(si.files());

        if (!engineFiles.isEmpty()) {
            for (String engineFile : engineFiles) {
                String compoundFile = engineFile + SparseConstants.COMPOUND_EXTENSION;
                dir.copyFrom(dir, engineFile, compoundFile, context);
            }
            segmentFiles.removeAll(engineFiles);
            si.setFiles(segmentFiles);
        }

        delegate.write(dir, si, context);
    }
}
