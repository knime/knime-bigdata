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
 *   Created on Feb 7, 2017 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark1_6.jobs.partition;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.junit.Test;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.util.EnumContainer.CountMethod;
import com.knime.bigdata.spark.core.job.util.EnumContainer.SamplingMethod;
import com.knime.bigdata.spark.node.preproc.sampling.SamplingJobInput;
import com.knime.bigdata.spark.node.preproc.sampling.SamplingJobOutput;
import com.knime.bigdata.spark1_6.api.SparkJob;
import com.knime.bigdata.spark1_6.jobs.AbstractSparkJobTest;
import com.knime.bigdata.spark1_6.jobs.preproc.partition.PartitionJob;

/**
 * {@link PartitionJob} Tests
 *
 * @author Sascha Wolke, KNIME.com
 */
@SuppressWarnings({"javadoc", "serial"})
public class PartitionJobTests extends AbstractSparkJobTest {

    private SparkJob<SamplingJobInput, SamplingJobOutput> getJob() {
        return new PartitionJob();
    }

    /////////// Absolute ///////////

    @Test
    public void absoluteFirst() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Absolute, 2, SamplingMethod.First, 0.1, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", sortByInteger(asRow(1, 2, 3, 4), 3));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertFalse("Samples ouput RDD is not input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertArrayEquals("First output", new Integer[] { 1, 2 }, asIntArray(outputA));
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new Integer[] { 3, 4 }, asIntArray(outputB));
    }

    /** Inverted partitions (samples over the half of all rows) */
    @Test
    public void absoluteFirstInverted() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Absolute, 51, SamplingMethod.First, 0.1, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", sortByInteger(asRow(intRange(1, 100)), 10));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertFalse("Samples ouput RDD is not input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertArrayEquals("First output", integerRange(1, 51), asSortedIntArray(outputA));
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", integerRange(52, 100), asSortedIntArray(outputB));
    }

    @Test
    public void absoluteFirstAll() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Absolute, 4, SamplingMethod.First, 0.1, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", sortByInteger(asRow(1, 2, 3, 4), 2));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertTrue("Samples ouput RDD is input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertNull("First output", outputA);
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new Integer[] { }, asIntArray(outputB));
    }

    @Test
    public void relativFirst() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Relative, 1, SamplingMethod.First, 0.5, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", sortByInteger(asRow(1, 2, 3, 4), 2));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertFalse("Samples ouput RDD is not input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertArrayEquals("First output", new Integer[] { 1, 2 }, asIntArray(outputA));
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new Integer[] { 3, 4 }, asIntArray(outputB));
    }

    @Test
    public void relativFirstAll() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Relative, 1, SamplingMethod.First, 1.0, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", sortByInteger(asRow(1, 2, 3, 4), 20));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertTrue("Samples ouput RDD is input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertNull("First output", outputA);
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new Integer[] { }, asIntArray(outputB));
    }

    /////////// Linear ///////////

    @Test(expected=KNIMESparkException.class)
    public void linearUnsupported() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Absolute, 2, SamplingMethod.Linear, 0.1, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", asRow(1, 2, 3, 4));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        job.runJob(javaSparkContext.sc(), input, namedObjecs);
    }

    /////////// RANDOM ///////////

    @Test
    public void absoluteRandom() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Absolute, 20, SamplingMethod.Random, 0.1, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", asRow(intRange(1, 40)));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertFalse("Samples ouput RDD is not input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertEquals("First output count", 20, outputA.count(), 2);
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertEquals("Second output count", 20, outputB.count(), 2);
        JavaRDD<Row> outputAll = outputA.union(outputB);
        assertArrayEquals("Contains all elements", ArrayUtils.toObject(intRange(1, 40)), asSortedIntArray(outputAll));
    }

    @Test
    public void absoluteRandomAll() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Absolute, 4, SamplingMethod.Random, 0.1, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", asRow(1, 2, 3, 4));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertTrue("Samples ouput RDD is input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertNull("First output", outputA);
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new Integer[0], asIntArray(outputB));
    }

    @Test
    public void relativRandom() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Relative, 1, SamplingMethod.Random, 0.5, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", asRow(intRange(1, 40)));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertFalse("Samples ouput RDD is not input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertEquals("First output count", 20, outputA.count(), 2);
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertEquals("Second output count", 20, outputB.count(), 2);
        JavaRDD<Row> outputAll = outputA.union(outputB);
        assertArrayEquals("Contains all elements", ArrayUtils.toObject(intRange(1, 40)), asSortedIntArray(outputAll));
    }

    @Test
    public void relativRandomAll() throws Exception {
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Relative, 1, SamplingMethod.Random, 1.0, 1, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", asRow(1, 2, 3, 4));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertTrue("Samples ouput RDD is input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertNull("First output", outputA);
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new Integer[0], asIntArray(outputB));
    }

    /////////// Stratified ///////////

    @Test
    public void stratifiedExact() throws Exception {
        int testData[] = new int[] {
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3
        };
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Relative, 1, SamplingMethod.Stratified, 0.5, 0, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", asRow(testData));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertFalse("Samples ouput RDD is not input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertArrayEquals("First output", new Integer[] { 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3 }, asIntArray(outputA));
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new Integer[] { 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3 }, asIntArray(outputB));
        JavaRDD<Row> outputAll = outputA.union(outputB);
        assertArrayEquals("Contains all elements", ArrayUtils.toObject(testData), asSortedIntArray(outputAll));
    }

    @Test
    public void stratifiedNotExact() throws Exception {
        int testData[] = new int[] {
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3
        };
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Relative, 1, SamplingMethod.Stratified, 0.5, 0, false, 123l, false);
        namedObjecs.addJavaRdd("inputA", asRow(testData));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertFalse("Samples ouput RDD is not input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        JavaRDD<Row> outputAll = outputA.union(outputB);
        assertArrayEquals("Contains all elements", ArrayUtils.toObject(testData), asSortedIntArray(outputAll));
    }

    @Test
    public void stratifiedNullValues() throws Exception {
        String testData[] = new String[] { null, "a", "a", null, "b", "b" };
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Relative, 1, SamplingMethod.Stratified, 0.5, 0, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", asRow(Arrays.asList(testData)));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertFalse("Samples ouput RDD is not input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertArrayEquals("First output", new String[] { null, "a", "b" }, asStringArray(outputA, true));
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new String[] { null, "a", "b" }, asStringArray(outputB, true));
        JavaRDD<Row> outputAll = outputA.union(outputB);
        assertArrayEquals("Contains all elements", new String[] { null, null, "a", "a", "b", "b" }, asStringArray(outputAll, true));
    }

    @Test
    public void stratifiedExactAll() throws Exception {
        int testData[] = new int[] {
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3
        };
        SamplingJobInput input = new SamplingJobInput("inputA", new String[] { "outputA", "outputB" },
            CountMethod.Relative, 1, SamplingMethod.Stratified, 1.0, 0, false, 123l, true);
        namedObjecs.addJavaRdd("inputA", asRow(testData));

        SparkJob<SamplingJobInput, SamplingJobOutput> job = getJob();
        SamplingJobOutput jobOutput = job.runJob(javaSparkContext.sc(), input, namedObjecs);

        assertTrue("Samples ouput RDD is input RDD", jobOutput.samplesRddIsInputRdd());
        JavaRDD<Row> outputA = namedObjecs.getJavaRdd("outputA");
        assertNull("First output", outputA);
        JavaRDD<Row> outputB = namedObjecs.getJavaRdd("outputB");
        assertArrayEquals("Second output", new Integer[0], asIntArray(outputB));
    }
}
