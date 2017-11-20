/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark1_2.jobs.preproc.sampling;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.api.java.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.EnumContainer.CountMethod;
import org.knime.bigdata.spark.node.preproc.sampling.SamplingJobInput;
import org.knime.bigdata.spark.node.preproc.sampling.SamplingJobOutput;
import org.knime.bigdata.spark1_2.api.NamedObjects;
import org.knime.bigdata.spark1_2.api.SparkJob;

import scala.Tuple2;

/**
 * Split input RDD into two RDDs using sampling.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public abstract class AbstractSamplingJob implements SparkJob<SamplingJobInput, SamplingJobOutput> {
    private static final long serialVersionUID = 1L;

    /** Internal logger */
    protected static final Logger LOGGER = Logger.getLogger(AbstractSamplingJob.class.getName());

    /**
     * Run the partitioning job.
     *
     * @throws KNIMESparkException if something goes wrong
     */
    @Override
    public SamplingJobOutput runJob(final SparkContext context, final SamplingJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException {

        LOGGER.info("Starting " + input.getSamplingMethod() + " sampling/partitioning job");

        switch (input.getSamplingMethod()) {
            case First:
                return fromTopSampling(context, input, namedObjects);
            case Random:
                return randomSampling(context, input, namedObjects);
            case Stratified:
                return stratifiedSampling(context, input, namedObjects);
            default:
                throw new KNIMESparkException("Unsupported sampling method: " + input.getSamplingMethod());
        }
    }

    /**
     * Returns input as output RDD job result and creates an empty second output if required.
     *
     * @param context spark context
     * @param input job input
     * @param namedObjects named objects handler
     * @return job output
     */
    protected SamplingJobOutput noSplitRequired(final SparkContext context, final SamplingJobInput input,
            final NamedObjects namedObjects) {

        LOGGER.info("No computation required, returning input RDD as sample RDD.");

        if (input.isSplit()) { // empty second output
            JavaSparkContext js = JavaSparkContext.fromSparkContext(context);
            JavaRDD<Row> emptyOutput = js.parallelize(Collections.<Row>emptyList());
            namedObjects.addJavaRdd(input.getNamedOutputObjects().get(1), emptyOutput);
        }

        return new SamplingJobOutput(true);
    }

    /**
     * Take first n samples from input RDD (all of first partition, than second until n samples taken).
     * <p/>
     * This implementation reads all data at least twice. We first count the rows per partition and than calculate at
     * which partition to split the rows.
     *
     * @param context Current context
     * @param input Job configuration
     * @param namedObjects Named objects handler
     * @return Job output
     * @throws KNIMESparkException
     */
    private SamplingJobOutput fromTopSampling(final SparkContext context, final SamplingJobInput input,
            final NamedObjects namedObjects) throws KNIMESparkException {

        final JavaRDD<Row> inputRdd = namedObjects.getJavaRdd(input.getFirstNamedInputObject());

        // Collect partition count
        final List<Tuple2<Integer, Long>> partitionCounts = inputRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Row>, Iterator<Tuple2<Integer, Long>>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Tuple2<Integer, Long>> call(final Integer partition, final Iterator<Row> iter) throws Exception {
                long i = 0;
                while (iter.hasNext()) {
                    i++;
                    iter.next();
                }
                return Collections.singleton(new Tuple2<>(partition, i)).iterator();
            }
        }, false).collect();

        // sum up overall rows count, count per partition and partitions count
        long rowCount = 0;
        final Map<Integer, Long> countPerPartition = new HashMap<>();
        for (Tuple2<Integer, Long> part : partitionCounts) {
            rowCount += part._2;
            countPerPartition.put(part._1, part._2);
        }
        final int partitions = countPerPartition.size();
        final long sampleCount;
        if (input.getCountMethod() == CountMethod.Absolute) {
            sampleCount = input.getCount();
        } else { // Relative
            sampleCount = (long) Math.ceil(rowCount * input.getFraction());
        }

        if (sampleCount >= rowCount) {
            return noSplitRequired(context, input, namedObjects);
        }

        // calculate partition to split on (lower partitions are samples, higher are others)
        long sampled = 0;
        int partition;
        for (partition = 0; partition < partitions && sampled < sampleCount; partition++) {
            sampled += countPerPartition.get(partition);
        }
        final int splitPartition = partition - 1;
        final long samplesInSplitPartition = sampleCount - (sampled - countPerPartition.get(splitPartition));

        final JavaRDD<Row> samples = inputRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Row>, Iterator<Row>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Row> call(final Integer curPartition, final Iterator<Row> rows) throws Exception {
                if (curPartition < splitPartition) {
                    return rows;

                } else if (curPartition == splitPartition) {
                    return new Iterator<Row>() {
                        private long id = 0;

                        @Override
                        public boolean hasNext() {
                            return rows.hasNext() && id < samplesInSplitPartition;
                        }

                        @Override
                        public Row next() {
                            id++;
                            return rows.next();
                        }
                    };

                } else {
                    return Collections.emptyListIterator();
                }
            }
        }, false);
        namedObjects.addJavaRdd(input.getNamedOutputObjects().get(0), samples);

        if (input.isSplit()) {
            final JavaRDD<Row> others = inputRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Row>, Iterator<Row>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Iterator<Row> call(final Integer curPartition, final Iterator<Row> rows) throws Exception {
                    if (curPartition < splitPartition) {
                        return Collections.emptyListIterator();

                    } else if (curPartition == splitPartition) {
                        // skip first elements in split partition
                        long id = 0;
                        while(rows.hasNext() && id < samplesInSplitPartition) {
                            id++;
                            rows.next();
                        }
                        return rows;

                    } else {
                        return rows;
                    }
                }
            }, false);
            namedObjects.addJavaRdd(input.getNamedOutputObjects().get(1), others);
        }

        return new SamplingJobOutput(false);
    }

    /**
     * Randomly choose samples by given fraction.
     *
     * @param context Current context
     * @param input Job configuration
     * @param namedObjects Named objects handler
     * @return Job output
     * @throws KNIMESparkException
     */
    protected abstract SamplingJobOutput randomSampling(final SparkContext context, final SamplingJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException;

    /**
     * Stratified split version using a given column as string label.
     *
     * @param context Current context
     * @param input Job configuration
     * @param namedObjects Named objects handler
     * @return Job output
     * @throws KNIMESparkException
     */
    protected abstract SamplingJobOutput stratifiedSampling(final SparkContext context, final SamplingJobInput input,
        final NamedObjects namedObjects) throws KNIMESparkException;

    /**
     * Calculates number of samples as fraction (might run count on input RDD!).
     *
     * @param input job configuration
     * @param inputRdd input rows
     * @return number of samples as fraction
     */
    protected double getFractionToSample(final SamplingJobInput input, final JavaRDD<Row> inputRdd) {
        if (input.getCountMethod() == CountMethod.Relative) {
            return input.getFraction();
        } else { // Absolute
            return Math.min(1d, ((double) input.getCount()) / inputRdd.count());
        }
    }
}
