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

package com.amazon.opendistroforelasticsearch.knn.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Path;

/**
 * File Listener class to perform hsnw index garbage collection when the corresponding
 * segments get deleted
 */
public class KNNIndexFileListener implements FileChangesListener {
    private static Logger logger = LogManager.getLogger(KNNIndexFileListener.class);

    private ResourceWatcherService resourceWatcherService;

    public KNNIndexFileListener(ResourceWatcherService resourceWatcherService) {
        this.resourceWatcherService= resourceWatcherService;
    }

    public void register(Path filePath) throws Exception {
        final FileWatcher watcher = new FileWatcher(filePath);
        watcher.addListener(this);
        watcher.init();
        resourceWatcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        logger.debug("[KNN] Registered file {}", filePath.toString());
    }

    @Override
    public void onFileDeleted(Path indexFilePath) {
        logger.debug("[KNN] Invalidated because file {} is deleted", indexFilePath.toString());
        KNNWeight.knnIndexCache.cache.invalidate(indexFilePath.toString());
    }
}
