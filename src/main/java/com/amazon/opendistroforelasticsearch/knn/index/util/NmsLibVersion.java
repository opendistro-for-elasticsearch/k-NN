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

import java.util.HashSet;
import java.util.Set;

public enum NmsLibVersion {

    /**
     * Latest available nmslib version
     */
    VNMSLIB_208("NMSLIB_208"){
        @Override
        public String indexLibraryVersion() {
            return "KNNIndex_NMSLIB_V2_0_8";
        }
    },
    /**
     * Latest available Faiss version
     */
    VFAISS_164("FAISS_164") {
        @Override
        public String indexLibraryVersion() {
            return "KNNIndex_FAISS_V1_6_4";
        }
    };

    public static final NmsLibVersion DEFAULT = VFAISS_164;
    public static final NmsLibVersion LATEST_NMSLIB = VNMSLIB_208;
    public static final NmsLibVersion LATEST_FAISS = VFAISS_164;
    public String buildVersion;

    NmsLibVersion(String buildVersion) {
        this.buildVersion = buildVersion;
    }

    /**
     * NMS library version used by the KNN codec
     * @return nmslib name
     */
    public abstract String indexLibraryVersion();

    public String getBuildVersion() { return buildVersion; }

    public static Set<String> getValues() {
        Set<String> values = new HashSet<>();
        for (NmsLibVersion libVersion : NmsLibVersion.values()) {
            values.add(libVersion.getBuildVersion());
        }
        return values;
    }

    public static NmsLibVersion getNmsLibVersion(String knnEngine) {
        if(VFAISS_164.getBuildVersion().contains(knnEngine)) {
            return VFAISS_164;
        } else {
            return VNMSLIB_208;
        }
    }
}
