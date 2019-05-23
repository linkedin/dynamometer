/**
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynamometer.blockgenerator;

import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This Mapper class generates a list of {@link BlockInfo}'s from a given
 * fsimage.
 *
 * Input: fsimage in XML format. It should be generated using
 * {@link org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer}.
 *
 * Output: list of all {@link BlockInfo}'s
 */
public class XMLParserMapper extends Mapper<LongWritable, Text, IntWritable, BlockInfo>  {

  private static final Log LOG = LogFactory.getLog(XMLParserMapper.class);

  @Override
  public void setup(Mapper.Context context) {
    Configuration conf = context.getConfiguration();
    numDataNodes = conf.getInt(GenerateBlockImagesDriver.NUM_DATANODES_KEY, -1);
    parser = new XMLParser();
  }

  //Blockindexes should be generated serially
  private int blockIndex = 0;
  private int numDataNodes;
  private XMLParser parser;

  /**
   * Read the input XML file line by line, and generate list of blocks.
   * The actual parsing logic is handled by {@link XMLParser}. This mapper
   * just delegates to that class and then writes the blocks to the corresponding
   * index to be processed by reducers.
   */
  @Override
  public void map(LongWritable lineNum, Text line,
      Mapper<LongWritable, Text, IntWritable, BlockInfo>.Context context)
      throws IOException, InterruptedException {
    List<BlockInfo> blockInfos;
    try {
      blockInfos = parser.parseLine(line.toString());
    } catch (IOException e) {
      throw new IOException(String.format("IOException %s happened for line %s", e.getMessage(), line));
    }

    for (BlockInfo blockInfo : blockInfos) {
      for (short i = 0; i < blockInfo.getReplication(); i++) {
        context.write(new IntWritable((blockIndex + i) % numDataNodes), blockInfo);
      }

      blockIndex++;
      if (blockIndex % 1000000 == 0) {
        LOG.info("Processed " + blockIndex + " blocks");
      }
    }
  }
}
