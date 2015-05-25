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

package com.cloudera.oryx.computation.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link DependenciesScheduler}.
 * 
 * @author Sean Owen
 */
public final class DependenciesSchedulerTest extends OryxTest {

  @Test
  public void testEmpty() {
    DependenciesScheduler<String> scheduler = new DependenciesScheduler<>();
    Collection<Collection<String>> schedule = scheduler.schedule(Collections.<DependsOn<String>>emptySet());
    assertTrue(schedule.isEmpty());
  }

  @Test
  public void testOne() {
    DependenciesScheduler<String> scheduler = new DependenciesScheduler<>();
    List<Collection<String>> schedule = scheduler.schedule(Collections.singleton(new DependsOn<>("foo")));
    assertEquals(1, schedule.size());
    assertEquals(Sets.newHashSet(Collections.singleton("foo")), Sets.newHashSet(schedule.get(0)));
  }

  @Test
  public void testSequence() {
    DependenciesScheduler<String> scheduler = new DependenciesScheduler<>();
    List<Collection<String>> schedule = scheduler.schedule(Collections.singleton(new DependsOn<>("foo", "bar")));
    assertEquals(2, schedule.size());
    assertEquals(Sets.newHashSet(Collections.singleton("bar")), Sets.newHashSet(schedule.get(0)));
    assertEquals(Sets.newHashSet(Collections.singleton("foo")), Sets.newHashSet(schedule.get(1)));
  }

  @Test
  public void testParallel() {
    Collection<DependsOn<String>> dependencies = Lists.newArrayListWithCapacity(2);
    dependencies.add(new DependsOn<>("foo"));
    dependencies.add(new DependsOn<>("bar"));
    DependenciesScheduler<String> scheduler = new DependenciesScheduler<>();
    List<Collection<String>> schedule = scheduler.schedule(dependencies);
    assertEquals(1, schedule.size());
    assertEquals(Sets.newHashSet(Arrays.asList("foo", "bar")), Sets.newHashSet(schedule.get(0)));
  }

  @Test
  public void testComplex() {
    Collection<DependsOn<String>> dependencies = Lists.newArrayListWithCapacity(4);
    dependencies.add(new DependsOn<>("foo", "bar"));
    dependencies.add(new DependsOn<>("foo", "buzz"));
    dependencies.add(new DependsOn<>("bing", "foo"));
    dependencies.add(new DependsOn<>("bong", "foo"));
    DependenciesScheduler<String> scheduler = new DependenciesScheduler<>();
    List<Collection<String>> schedule = scheduler.schedule(dependencies);
    assertEquals(3, schedule.size());
    assertEquals(Sets.newHashSet(Arrays.asList("bar", "buzz")), Sets.newHashSet(schedule.get(0)));
    assertEquals(Sets.newHashSet(Collections.singleton("foo")), Sets.newHashSet(schedule.get(1)));
    assertEquals(Sets.newHashSet(Arrays.asList("bing", "bong")), Sets.newHashSet(schedule.get(2)));
  }

}
