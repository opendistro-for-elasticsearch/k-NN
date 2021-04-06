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

public enum NmsLibVersion {

    /**
     * Latest available nmslib version
     */
    VNMSLIB_2011("NMSLIB_2011"){
        @Override
        public String indexLibraryVersion() {
            return "KNNIndex_NMSLIB_V2_0_11";
        }
    };

    public static final NmsLibVersion LATEST = VNMSLIB_2011;

    private String buildVersion;

    NmsLibVersion(String buildVersion) {
        this.buildVersion = buildVersion;
    }

    /**
     * NMS library version used by the KNN codec
     * @return nmslib name
     */
    public abstract String indexLibraryVersion();

    public String getBuildVersion() { return buildVersion; }
}
