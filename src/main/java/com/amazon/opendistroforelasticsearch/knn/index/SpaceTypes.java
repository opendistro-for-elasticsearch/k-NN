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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Enum contains space types for k-NN similarity search
 */
public enum SpaceTypes {
  l2("l2"),
  cosinesimil("cosinesimil"),
  negdotprod("negdotprod"),
  bit_hamming("bit_hamming");
  private String value;

  SpaceTypes(String value) { this.value = value; }

  /**
   * Get space type
   *
   * @return name
   */
  public String getValue() { return value; }

  /**
   * Get all space types
   *
   * @return set of all stat names
   */
  public static Set<String> getValues() {
    Set<String> values = new HashSet<>();

    for (SpaceTypes spaceType : SpaceTypes.values()) {
      values.add(spaceType.getValue());
    }
    return values;
  }

  /**
   * Get space types not supporting optimized index.
   * https://github.com/nmslib/nmslib/blob/master/python_bindings/README.md#saving-indexes-and-data
   *
   * @return set of all stat names
   */
  public static Set<String> getOptimizedValues() {
    return new HashSet<>(Arrays.asList(cosinesimil.getValue(), l2.getValue()));
  }

  public static Set<String> getStringSpaces() {
    return new HashSet<>(Arrays.asList(bit_hamming.getValue()));
  }
}
