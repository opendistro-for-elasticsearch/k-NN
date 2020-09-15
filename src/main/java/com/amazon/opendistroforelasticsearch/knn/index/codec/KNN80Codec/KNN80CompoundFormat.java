/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.knn.index.codec.KNN80Codec;

import com.amazon.opendistroforelasticsearch.knn.index.codec.KNNCodecUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to encode/decode compound file
 */
public class KNN80CompoundFormat extends CompoundFormat {

    private final Logger logger = LogManager.getLogger(KNN80CompoundFormat.class);

    public KNN80CompoundFormat() {
    }

    @Override
    public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        return Codec.getDefault().compoundFormat().getCompoundReader(dir, si, context);
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        /**
         * If hnsw file present, remove it from the compounding file list to avoid header/footer checks
         * and create a new compounding file format with extension .hnswc.
         */
        Set<String> hnswFiles = si.files().stream().filter(file -> file.endsWith(KNNCodecUtil.HNSW_EXTENSION))
                                     .collect(Collectors.toSet());

        Set<String> segmentFiles = new HashSet<>();
        segmentFiles.addAll(si.files());

        if (!hnswFiles.isEmpty()) {
            for (String hnswFile: hnswFiles) {
                String hnswCompoundFile = hnswFile + "c";
                dir.copyFrom(dir, hnswFile, hnswCompoundFile, context);
            }
            segmentFiles.removeAll(hnswFiles);
            si.setFiles(segmentFiles);
        }
        Codec.getDefault().compoundFormat().write(dir, si, context);
    }
}
