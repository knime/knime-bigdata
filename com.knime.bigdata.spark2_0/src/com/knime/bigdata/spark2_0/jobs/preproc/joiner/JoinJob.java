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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark2_0.jobs.preproc.joiner;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.job.util.MyJoinKey;
import com.knime.bigdata.spark.node.preproc.joiner.JoinMode;
import com.knime.bigdata.spark.node.preproc.joiner.SparkJoinerJobInput;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.RDDUtilsInJava;
import com.knime.bigdata.spark2_0.api.SimpleSparkJob;
import com.knime.bigdata.spark2_0.api.TypeConverters;

import scala.Tuple2;

/**
 * Joins two data frames and into a new one.
 *
 * @author dwk
 */
@SparkClass
public class JoinJob implements SimpleSparkJob<SparkJoinerJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(JoinJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final SparkJoinerJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        final JoinMode mode = input.getJoineMode();
        LOGGER.info("Joining via " + mode.toString() + " two data frames...");

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final List<Integer> joinIdxLeft = Arrays.asList(input.getJoinColIdxsLeft());
        JavaPairRDD<MyJoinKey, Row> leftRdd =
            RDDUtilsInJava.extractKeys(namedObjects.getJavaRdd(input.getLeftInputObject()),
                joinIdxLeft.toArray(new Integer[joinIdxLeft.size()]));
        final List<Integer> joinIdxRight = Arrays.asList(input.getJoinColIdxsRight());
        JavaPairRDD<MyJoinKey, Row> rightRdd =
            RDDUtilsInJava.extractKeys(namedObjects.getJavaRdd(input.getRightNamedObject()),
                joinIdxRight.toArray(new Integer[joinIdxRight.size()]));

        final List<Integer> colIdxLeft = Arrays.asList(input.getSelectColIdxsLeft());
        final List<Integer> colIdxRight = Arrays.asList(input.getSelectColIdxsRight());

        final JavaRDD<Row> resultRdd;
        switch (mode) {
            case InnerJoin: {
                JavaRDD<Tuple2<Row, Row>> joinedRdd = leftRdd.join(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case LeftOuterJoin: {
                JavaRDD<Tuple2<Row, Optional<Row>>> joinedRdd = leftRdd.leftOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case RightOuterJoin: {
                JavaRDD<Tuple2<Optional<Row>, Row>> joinedRdd = leftRdd.rightOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case FullOuterJoin: {
                JavaRDD<Tuple2<Optional<Row>, Optional<Row>>> joinedRdd = leftRdd.fullOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            default: {
                throw new KNIMESparkException("ERROR: unsupported join mode: " + mode);
            }
        }

        LOGGER.info("Storing join result under key: " + input.getFirstNamedOutputObject());
        final String resultKey = input.getFirstNamedOutputObject();
        final StructType resultSchema = TypeConverters.convertSpec(input.getSpec(resultKey));
        final Dataset<Row> result = spark.createDataFrame(resultRdd, resultSchema);
        namedObjects.addDataFrame(resultKey, result);
    }
}
