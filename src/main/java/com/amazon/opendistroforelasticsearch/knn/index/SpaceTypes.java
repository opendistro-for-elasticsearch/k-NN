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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Enum contains space type key-value pairs for k-NN similarity search.
 * key represents the space type name exposed to user;
 * value represents the internal space type name used in nmslib.
 */
public enum SpaceTypes {
  l2("l2", "l2"),
  cosinesimil("cosinesimil", "cosinesimil"),
  l1("l1", "l1"),
  linf("linf", "linf"),
  inner_product("inner_product", "negdotprod");

  private static final Map<String, String> TRANSLATION = new HashMap<>();

  private final String key;
  private final String value;

  static {
    for (SpaceTypes spaceType : values()) {
      TRANSLATION.put(spaceType.key, spaceType.value);
    }
  }

  public static boolean contains(final String name) { return TRANSLATION.containsKey(name); }

  public static String getValueByKey(final String name) { return TRANSLATION.get(name); }

  SpaceTypes(String key, String value) {
    this.key = key;
    this.value = value;
  }

  /**
   * Get space type name in KNN plugin
   *
   * @return name
   */
  public String getKey() { return key; }

  /**
   * Get space type name in nmslib
   *
   * @return name
   */
  public String getValue() { return value; }
}
