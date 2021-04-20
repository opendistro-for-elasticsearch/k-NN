/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.knn.index.util;

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.common.KNNConstants;

public class KNNEngineTests extends KNNTestCase {
    /**
     * Get latest build version from library
     */
    public void testDelegateLibraryFunctions() {
        assertEquals(KNNLibrary.Faiss.INSTANCE.getLatestBuildVersion(), KNNEngine.FAISS.getLatestBuildVersion());
        assertEquals(KNNLibrary.Nmslib.INSTANCE.getLatestLibVersion(), KNNEngine.NMSLIB.getLatestLibVersion());
    }

    /**
     * Test name getter
     */
    public void testGetName() {
        assertEquals(KNNConstants.NMSLIB_NAME, KNNEngine.NMSLIB.getName());
        assertEquals(KNNConstants.FAISS_NAME, KNNEngine.FAISS.getName());
    }

    /**
     * Test engine getter
     */
    public void testGetEngine() {
        assertEquals(KNNEngine.NMSLIB, KNNEngine.getEngine(KNNConstants.NMSLIB_NAME));
        assertEquals(KNNEngine.FAISS, KNNEngine.getEngine(KNNConstants.FAISS_NAME));
        expectThrows(IllegalArgumentException.class, () -> KNNEngine.getEngine("invalid"));
    }
}
