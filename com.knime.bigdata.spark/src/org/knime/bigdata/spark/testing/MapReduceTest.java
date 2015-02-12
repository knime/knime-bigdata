/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 27.01.2015 by koetter
 */
package org.knime.bigdata.spark.testing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MRConfig;
/**
 *
 * @author koetter
 */
public class MapReduceTest {

    /**
     */
    public static void main(final String[] args) throws IOException {
//        System.setProperty("HADOOP_CONF_DIR",
//            "C:\\DEVELOPMENT\\workspaces\\trunk\\com.knime.bigdata.spark\\conf\\hadoop");
        Configuration config = new Configuration();
        // this should be like defined in your mapred-site.xml
//        config.set("mapred.job.tracker", "sandbox.hortonworks.com:50001");
        // like defined in hdfs-site.xml
        config.set("fs.default.name", "hdfs://sandbox.hortonworks.com:8020");
        //for YARN
        // this should be like defined in your yarn-site.xml
//        config.set("yarn.resourcemanager.address", "sandbox.hortonworks.com:8050");
        // framework is now "yarn", should be defined like this in mapred-site.xm
        config.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);


        JobConf job = new JobConf(config);
        job.setJarByClass(MapReduceTest.class);
        job.setJobName("My first job");

        FileInputFormat.setInputPaths(job, new Path("/mytemp"));
        FileOutputFormat.setOutputPath(job, new Path("/mytemp/out"));
        System.getenv("");

        job.setMapperClass(MapReduceTest.MyFirstMapper.class);
        job.setReducerClass(MapReduceTest.MyFirstReducer.class);

        JobClient.runJob(job);
    }
    private static class MyFirstMapper extends MapReduceBase implements Mapper {
        public void map(final LongWritable key, final Text value, final OutputCollector output, final Reporter reporter) throws IOException {

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void map(final Object arg0, final Object arg1, final OutputCollector arg2, final Reporter arg3) throws IOException {
            // TK_TODO Auto-generated method stub

        }
    }

    private static class MyFirstReducer extends MapReduceBase implements Reducer {
        public void reduce(final Text key, final Iterator values, final OutputCollector output, final Reporter reporter) throws IOException {

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void reduce(final Object arg0, final Iterator arg1, final OutputCollector arg2, final Reporter arg3) throws IOException {
            // TK_TODO Auto-generated method stub

        }
    }
}
