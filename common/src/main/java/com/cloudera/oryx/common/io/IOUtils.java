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

package com.cloudera.oryx.common.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipInputStream;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.PatternFilenameFilter;

/**
 * Simple utility methods related to I/O.
 *
 * @author Sean Owen
 */
public final class IOUtils {

  /**
   * A {@link FilenameFilter} that accepts files whose name does not start with "."
   */
  public static final FilenameFilter NOT_HIDDEN = new PatternFilenameFilter("^[^.].*");

  private IOUtils() {
  }

  /**
   * Attempts to recursively delete a directory. This may not work across symlinks.
   *
   * @param dir directory to delete along with contents
   * @throws IOException if any deletion fails
   */
  public static void deleteRecursively(File dir) throws IOException {
    if (dir == null || !dir.exists()) {
      return;
    }
    Deque<File> stack = new ArrayDeque<>();
    stack.push(dir);
    while (!stack.isEmpty()) {
      File topElement = stack.peek();
      if (topElement.isDirectory()) {
        File[] directoryContents = topElement.listFiles();
        if (directoryContents != null && directoryContents.length > 0) {
          for (File fileOrSubDirectory : directoryContents) {
            stack.push(fileOrSubDirectory);
          }
        } else {
          delete(stack.pop());
        }
      } else {
        delete(stack.pop());
      }
    }
  }

  /**
   * Like {@link File#delete()} but throws an exception if the operation fails.
   *
   * @param file file to delete
   * @throws IOException if deletion fails
   */
  public static void delete(File file) throws IOException {
    if (file.exists() && !file.delete()) {
      throw new IOException("Failed to delete " + file);
    }
  }

  /**
   * Opens an {@link InputStream} to the file. If it appears to be compressed, because its file name ends in
   * ".gz" or ".zip", then it will be decompressed accordingly
   *
   * @param file file, possibly compressed, to open
   * @return {@link InputStream} on uncompressed contents
   * @throws IOException if the stream can't be opened or is invalid or can't be read
   */
  public static InputStream openMaybeDecompressing(File file) throws IOException {
    String name = file.getName();
    InputStream in = new FileInputStream(file);
    if (name.endsWith(".gz")) {
      return new GZIPInputStream(in);
    }
    if (name.endsWith(".zip")) {
      return new ZipInputStream(in);
    }
    return in;
  }
  
  /**
   * @param file file, possibly compressed, to open
   * @return {@link Reader} on uncompressed contents
   * @throws IOException if the stream can't be opened or is invalid or can't be read
   * @see #openMaybeDecompressing(File)
   */
  public static Reader openReaderMaybeDecompressing(File file) throws IOException {
    return new InputStreamReader(openMaybeDecompressing(file), Charsets.UTF_8);
  }

  /**
   * @param in   stream to read and copy
   * @param file file to write stream's contents to
   * @throws IOException if the stream can't be read or the file can't be written
   */
  public static void copyStreamToFile(InputStream in, File file) throws IOException {
    try (FileOutputStream out = new FileOutputStream(file)) {
      ByteStreams.copy(in, out);
    }
  }

  /**
   * @param url  URL whose contents are to be read and copied
   * @param file file to write contents to
   * @throws IOException if the URL can't be read or the file can't be written
   */
  public static void copyURLToFile(URL url, File file) throws IOException {
    try (InputStream in = url.openStream()) {
      copyStreamToFile(in, file);
    }
  }

  /**
   * Like {@link File#mkdirs()} but throws {@link IOException} if the operation fails.
   */
  public static void mkdirs(File dir) throws IOException {
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Failed to create " + dir);
    }
  }

  /**
   * @param delegate {@link OutputStream} to wrap
   * @return a {@link GZIPOutputStream} wrapping the given {@link OutputStream}
   */
  public static GZIPOutputStream buildGZIPOutputStream(OutputStream delegate) throws IOException {
    return new GZIPOutputStream(delegate, true);
  }

  /**
   * @param delegate {@link OutputStream} to wrap
   * @return the result of {@link #buildGZIPOutputStream(OutputStream)} as a {@link Writer} that encodes
   *  using UTF-8 encoding
   */
  public static Writer buildGZIPWriter(OutputStream delegate) throws IOException {
    return new OutputStreamWriter(buildGZIPOutputStream(delegate), Charsets.UTF_8);
  }

  /**
   * @see #buildGZIPWriter(OutputStream)
   */
  public static Writer buildGZIPWriter(File file) throws IOException {
    return buildGZIPWriter(new FileOutputStream(file, false));
  }

  /**
   * Wraps its argument in {@link BufferedReader} if not already one.
   */
  public static BufferedReader buffer(Reader maybeBuffered) {
    return maybeBuffered instanceof BufferedReader 
        ? (BufferedReader) maybeBuffered 
        : new BufferedReader(maybeBuffered);
  }

  /**
   * @return true iff the given file is a gzip-compressed file with no content; the file itself may not
   *  be empty because it contains gzip headers and footers
   * @throws IOException if the file is not a gzip file or can't be read
   */
  public static boolean isGZIPFileEmpty(File f) throws IOException {
    try (InputStream in = new GZIPInputStream(new FileInputStream(f))) {
      return in.read() == -1;
    }
  }

}
