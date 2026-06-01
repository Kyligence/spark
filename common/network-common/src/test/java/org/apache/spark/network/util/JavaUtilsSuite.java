/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.InputStream;

import org.junit.Test;
import static org.junit.Assert.*;

public class JavaUtilsSuite {

  @Test
  public void testSkipFullyZeroBytes() throws Exception {
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    InputStream in = new ByteArrayInputStream(data);
    JavaUtils.skipFully(in, 0);
    assertEquals(1, in.read());
  }

  @Test
  public void testSkipFullyWithinAvailable() throws Exception {
    byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InputStream in = new ByteArrayInputStream(data);
    JavaUtils.skipFully(in, 3);
    assertEquals(4, in.read());
    assertEquals(5, in.read());
    assertEquals(6, in.read());
  }

  @Test
  public void testSkipFullyLargerThanSkipBuffer() throws Exception {
    byte[] data = new byte[20000];
    data[0] = 1;
    data[9999] = 99;
    data[10000] = 100;
    data[19999] = 127;

    InputStream in = new ByteArrayInputStream(data);
    JavaUtils.skipFully(in, 10000);
    assertEquals(100, in.read());
  }

  @Test
  public void testSkipFullyExactSize() throws Exception {
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    InputStream in = new ByteArrayInputStream(data);
    JavaUtils.skipFully(in, 5);
    assertEquals(-1, in.read());
  }

  @Test(expected = EOFException.class)
  public void testSkipFullyBeyondEOF() throws Exception {
    byte[] data = new byte[] {1, 2, 3};
    InputStream in = new ByteArrayInputStream(data);
    JavaUtils.skipFully(in, 10);
  }

  @Test
  public void testSkipFullyThenReadRemaining() throws Exception {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    InputStream in = new ByteArrayInputStream(data);
    JavaUtils.skipFully(in, 7);
    byte[] remaining = new byte[3];
    int bytesRead = 0;
    while (bytesRead < remaining.length) {
      int n = in.read(remaining, bytesRead, remaining.length - bytesRead);
      if (n == -1) break;
      bytesRead += n;
    }
    assertArrayEquals(new byte[] {7, 8, 9}, remaining);
  }

  @Test
  public void testSkipFullySkipZeroIgnoresFirstReadError() throws Exception {
    InputStream in = new InputStream() {
      private int remaining = 10;
      @Override
      public int read() {
        if (remaining <= 0) return -1;
        remaining--;
        return remaining;
      }
      @Override
      public long skip(long n) {
        return 0;
      }
    };
    JavaUtils.skipFully(in, 5);
    assertEquals(4, in.read());
  }
}
