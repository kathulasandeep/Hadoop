package com.cloudwick.sandeep.IPMapping;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class MyRecordReader extends RecordReader<LongWritable, CustomWritable> {
	  private static final Log LOG = LogFactory.getLog(MyRecordReader.class);

	  private CompressionCodecFactory compressionCodecs = null;
	  private long start;
	  private long pos;
	  private long end;
	  private LineReader in;
	  private int maxLineLength;
	  private LongWritable key = null;
	  private CustomWritable value = null;
	  private Text val2=new Text();
	  private Text val=new Text();
	  Pattern p;

	  public void initialize(InputSplit genericSplit,
	                         TaskAttemptContext context) throws IOException {
	    FileSplit split = (FileSplit) genericSplit;
	    Configuration job = context.getConfiguration();
	    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
	                                    Integer.MAX_VALUE);
	    //p = Pattern.compile("^(\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3})\\s.*\\s(.*)\\s(.*)\\s\"(.*)\\s.*\"\\s(\\d{3}).*$");
	    p = Pattern.compile("^(\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3})\\s.*\\s(.*)\\s(.*\\s.*)\\s\"(.*)\\s/.*\"\\s(\\d{3}).*$");
	    start = split.getStart();
	    end = start + split.getLength();
	    final Path file = split.getPath();
	    compressionCodecs = new CompressionCodecFactory(job);
	    final CompressionCodec codec = compressionCodecs.getCodec(file);

	    // open the file and seek to the start of the split
	    FileSystem fs = file.getFileSystem(job);
	    FSDataInputStream fileIn = fs.open(split.getPath());
	    boolean skipFirstLine = false;
	    if (codec != null) {
	      in = new LineReader(codec.createInputStream(fileIn), job);
	      end = Long.MAX_VALUE;
	    } else {
	      if (start != 0) {
	        skipFirstLine = true;
	        --start;
	        fileIn.seek(start);
	      }
	      in = new LineReader(fileIn, job);
	    }
	    if (skipFirstLine) {  // skip first line and re-establish "start".
	      start += in.readLine(new Text(), 0,
	                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
	    }
	    this.pos = start;
	  }
	  
	  public boolean nextKeyValue() throws IOException {
	    if (key == null) {
	      key = new LongWritable();
	    }
	    key.set(pos);
	    if (value == null) {
	      value = new CustomWritable();
	    }
	    int newSize = 0;
	    while (pos < end) {
	      newSize = in.readLine(val2, maxLineLength,
	                            Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
	                                     maxLineLength));
	      Matcher m = p.matcher(val2.toString());
	      m.find();
	 
	      val.set(m.group(1));
	      value.setIp(new Text(m.group(1)));
	      val.set(m.group(2));
	      value.setUserId(new Text(m.group(2)));
	      val.set(m.group(3));
	      value.setTimeStamp(new Text(m.group(3)));
	      val.set(m.group(4));
	      value.setRequestType(new Text(m.group(4)));
	      val.set(m.group(5));
	      value.setResponceCode(new Text(m.group(5)));
	     
	      if (newSize == 0) {
	        break;
	      }
	      pos += newSize;
	      if (newSize < maxLineLength) {
	        break;
	      }

	      // line too long. try again
	      LOG.info("Skipped line of size " + newSize + " at pos " + 
	               (pos - newSize));
	    }
	    if (newSize == 0) {
	      key = null;
	      value = null;
	      return false;
	    } else {
	      return true;
	    }
	  }

	  @Override
	  public LongWritable getCurrentKey() {
	    return key;
	  }

	  @Override
	  public CustomWritable getCurrentValue() {
	    return value;
	  }

	  /**
	   * Get the progress within the split
	   */
	  public float getProgress() {
	    if (start == end) {
	      return 0.0f;
	    } else {
	      return Math.min(1.0f, (pos - start) / (float)(end - start));
	    }
	  }
	  
	  public synchronized void close() throws IOException {
	    if (in != null) {
	      in.close(); 
	    }
	  }
	}
