/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.als.common.candidate;

import java.util.concurrent.locks.Lock;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.ClassUtils;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * <p>This class helps choose which {@link CandidateFilter} to apply to the recommendation process.
 * If the "model.candidateFilter.customClass" system property is set, then this class will be loaded and used.
 * See notes in {@link CandidateFilter} about how the class must be implemented.</p>
 * 
 * <p>Otherwise, if "model.lsh.sample-ratio" is set to a value less than 1, then {@link LocationSensitiveHashFilter}
 * will be used. It is a somewhat special case, a built-in type of filter.</p>
 * 
 * <p>Otherwise an implementation that does no filtering will be returned.</p>
 * 
 * @author Sean Owen
 */
public final class CandidateFilterFactory {

  private final String candidateFilterClassName;
  private final StringLongMapping idMapping;

  public CandidateFilterFactory(StringLongMapping idMapping) {
    Config config = ConfigUtils.getDefaultConfig();
    candidateFilterClassName =
        config.hasPath("serving-layer.candidate-filter-class") ?
        config.getString("serving-layer.candidate-filter-class") : null;
    
    this.idMapping = idMapping;
  }

  /**
   * @return an implementation of {@link CandidateFilter} chosen per above. It will be non-null.
   * 
   * @param Y item-feature matrix
   * @param yReadLock read lock that should be acquired to access {@code Y}
   */
  public CandidateFilter buildCandidateFilter(LongObjectMap<float[]> Y, Lock yReadLock) {
    Preconditions.checkNotNull(Y);
    if (!Y.isEmpty() && candidateFilterClassName != null) {
      yReadLock.lock();
      try {
        return ClassUtils.loadInstanceOf(candidateFilterClassName,
                                         CandidateFilter.class,
                                         new Class<?>[]{LongObjectMap.class, StringLongMapping.class},
                                         new Object[]{Y, idMapping});
      } catch (IllegalStateException e) {
        return ClassUtils.loadInstanceOf(candidateFilterClassName,
                                         CandidateFilter.class,
                                         new Class<?>[]{LongObjectMap.class},
                                         new Object[]{Y});
      } finally {
        yReadLock.unlock();
      }
    }
    return new IdentityCandidateFilter(Y);    
  }
  
}
