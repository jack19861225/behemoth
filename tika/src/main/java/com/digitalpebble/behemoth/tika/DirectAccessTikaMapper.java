/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.digitalpebble.behemoth.tika;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;
import com.digitalpebble.behemoth.util.CorpusGenerator.Counters;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

/**
 * Uses a {@link com.digitalpebble.behemoth.tika.TikaProcessor} to extract text
 * using Tika. Users wanting to override the default work of the TikaProcessor
 * can set the "tika.processor" value in the JobConf and give it a fully
 * qualified class name. The implementation must extend TikaProcessor and it
 * must have a zero arg. constructor.
 */
public class DirectAccessTikaMapper extends MapReduceBase implements
        Mapper<Text, Text, Text, BehemothDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(DirectAccessTikaMapper.class);

    protected DocumentProcessor processor;
    int MAX_SIZE = 1024 * 1024 * 20; // XXX this should be configurable!
    protected JobConf conf;
    private Text key = new Text();

    @Override
    public void map(Text path, Text type,
            OutputCollector<Text, BehemothDocument> outputCollector,
            Reporter reporter) throws IOException {
        String pathString = path.toString();
        try {
          pathString = URLDecoder.decode(path.toString(), "UTF-8");
        } catch (Exception e) {
          LOG.warn("Invalid URLEncoded string, file might be inaccessible: " + e.toString());
          pathString = path.toString();
        }
//        try {
//          URI u = new URI(pathString);
//          u = u.normalize();
//          pathString = u.toString();
//        } catch (Exception e) {
//          LOG.warn("Invalid URI, file might be inaccessible: " + e.toString());
//        }
        Path p = new Path(pathString);
        FileSystem fs = p.getFileSystem(conf);
        if (!fs.exists(p)) {
          LOG.warn("File could not be found! " + p.toUri());
          if (reporter != null)
            reporter.getCounter("TIKA", "NOT_FOUND");
          return;
        }
        String uri = p.toUri().toString();
        int processed = 0;
        String fn = p.getName().toLowerCase(Locale.ENGLISH);
        if (type.toString().equals("seq")) {
          // XXX add code to subcrawl sequence files
        } else if (type.toString().equals("map")) {
          // XXX add code to subcrawl map files
        }
        if (processed == 0 && isArchive(fn)) {
          InputStream fis = null;
          try {
            fis = fs.open(p);
            if (fn.endsWith(".gz") || fn.endsWith(".tgz")) {
              fis = new GZIPInputStream(fis);
            } else if (fn.endsWith(".tbz") || fn.endsWith(".tbz2") || fn.endsWith(".bzip2")) {
              fis = new BZip2CompressorInputStream(fis);
            }
            ArchiveInputStream input = new ArchiveStreamFactory()
            .createArchiveInputStream(new BufferedInputStream(
                    fis));
            ArchiveEntry entry = null;
            while ((entry = input.getNextEntry()) != null) {
              String name = entry.getName();
              long size = entry.getSize();
              byte[] content = new byte[(int) size];
              input.read(content);
              key.set(uri + "!" + name);
              // fill the values for the content object
              BehemothDocument value = new BehemothDocument();
              value.setUrl(uri + ":" + name);
              value.setContent(content);
              processed++;
              BehemothDocument[] documents = processor.process(value, reporter);
              if (documents != null) {
                for (int i = 0; i < documents.length; i++) {
                    outputCollector.collect(key, documents[i]);
                }
              }
            }
          } catch (Throwable t) {
            if (processed == 0) {
              LOG.warn("Error unpacking archive: " + p + ", adding as a regular file: " + t.toString());
            } else {
              LOG.warn("Error unpacking archive: " + p + ", processed " + processed + " entries, skipping remaining entries: " + t.toString());                            
            }
          } finally {
            if (fis != null) {
              fis.close();
            }
          }
        }
        if (processed == 0) { // not an archive or failed
          int realSize = (int) fs.getFileStatus(p).getLen();
          int maxLen = Math.min(MAX_SIZE, realSize);
          byte[] fileBArray = new byte[maxLen];
          FSDataInputStream fis = null;
          try {
            fis = fs.open(p);
            fis.readFully(0, fileBArray);
            fis.close();
            key.set(uri);
            // fill the values for the content object
            BehemothDocument value = new BehemothDocument();
            value.setUrl(uri);
            value.setContent(fileBArray);
            if (realSize > maxLen) {
              value.getMetadata(true).put(new Text("fetch"), new Text("truncated " + realSize + " to " + maxLen + " bytes."));
            }
            BehemothDocument[] documents = processor.process(value, reporter);
            if (documents != null) {
              for (int i = 0; i < documents.length; i++) {
                  outputCollector.collect(key, documents[i]);
              }
            }
          } catch (FileNotFoundException e) {
            LOG.warn("File not found " + p + ", skipping: " + e);
          } catch (IOException e) {
            LOG.warn("IO error reading file " + p + ", skipping: " + e);
          } finally {
            if (fis != null) {
              fis.close();
            }
          }
        }
    }
    
    private static boolean isArchive(String n) {
      if (  n.endsWith(".cpio") || n.endsWith(".jar") ||
              n.endsWith(".dump") || n.endsWith(".ar") ||
              n.endsWith("tar") ||
              n.endsWith(".zip") || n.endsWith("tar.gz") ||
              n.endsWith(".tgz") || n.endsWith(".tbz2") ||
              n.endsWith(".tbz") || n.endsWith("tar.bzip2")) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void configure(JobConf job) {
        this.conf = job;
        String handlerName = job.get(TikaConstants.TIKA_PROCESSOR_KEY);
        LOG.info("Configured DocumentProcessor class: " + handlerName);
        if (handlerName != null) {
            Class handlerClass = job.getClass(TikaConstants.TIKA_PROCESSOR_KEY, DocumentProcessor.class);
            try {
                processor = (DocumentProcessor) handlerClass.newInstance();

            } catch (InstantiationException e) {
                LOG.error("Exception", e);
                // TODO: what's the best way to do this?
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                LOG.error("Exception", e);
                throw new RuntimeException(e);
            }
        } else {
            processor = new TikaProcessor();
        }
        LOG.info("Using DocumentProcessor class: " + processor.getClass().getName());
        processor.setConf(job);
    }
}
