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
package com.knime.bigdata.spark.jobserver.jobs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
public class ConvertNominalValuesJob extends AbstractStringMapperJob {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_MAPPING_TYPE = ParameterConstants.PARAM_STRING;

    private static final String PARAM_RESULT_MAPPING =ParameterConstants.PARAM_TABLE_2;

    private final static Logger LOGGER = Logger.getLogger(ConvertNominalValuesJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = super.validateParam(aConfig);

        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_MAPPING_TYPE)) {
                msg = "Input parameter '" + PARAM_MAPPING_TYPE + "' missing.";
            } else {
                try {
                    MappingType.valueOf(aConfig.getInputParameter(PARAM_MAPPING_TYPE));
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_MAPPING_TYPE + "' has an invalid value.";
                }
            }
        }

        if (msg == null) {
            if (!aConfig.hasOutputParameter(PARAM_RESULT_MAPPING)) {
                msg = "Output parameter '" + PARAM_RESULT_MAPPING + "' missing.";
            }
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * @param aContext
     * @param aConfig
     * @param aRowRDD
     * @param aColIds
     * @param aColNameForIndex
     * @param aMappingType
     * @return
     */
    @Override
    JobResult execute(final SparkContext aContext, final JobConfig aConfig, final JavaRDD<Row> aRowRDD,
        final int[] aColIds, final Map<Integer, String> aColNameForIndex)
        throws GenericKnimeSparkException {
        final MappingType mappingType = MappingType.valueOf(aConfig.getInputParameter(PARAM_MAPPING_TYPE));

        //use only the column indices when converting
        final MappedRDDContainer mappedData =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(aRowRDD, aColIds, mappingType);

        LOGGER
            .log(Level.INFO, "Storing mapped data under key: " + aConfig.getOutputStringParameter(PARAM_RESULT_TABLE));

        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), mappedData.m_RddWithConvertedValues);

        //number of all (!)  columns in input data table
        int offset = aRowRDD.take(1).get(0).length();

        storeMappingsInRdd(aContext, mappedData, aColNameForIndex, aConfig.getOutputStringParameter(PARAM_RESULT_MAPPING), offset);
        return JobResult.emptyJobResult().withMessage("OK").withObjectResult(mappedData);
    }

    /**
     * stores the mapping in a RDD
     *
     * note that we are adding two rows for each mapping - one with the original column name and one for the new numeric
     * column name
     *
     * @param aSparkContext
     * @param aMappedData
     * @param aColNameForIndex
     * @param aRddName
     * @param aMappingType
     * @param aOffset
     */
    private void storeMappingsInRdd(final SparkContext aSparkContext, final MappedRDDContainer aMappedData,
        final Map<Integer, String> aColNameForIndex, final String aRddName, final int aOffset) {
        @SuppressWarnings("resource")
        JavaSparkContext javaContext = new JavaSparkContext(aSparkContext);

        List<Row> rows = aMappedData.createMappingTable(aColNameForIndex, aOffset);

        JavaRDD<Row> mappingRdd = javaContext.parallelize(rows);
        LOGGER.log(Level.INFO, "Storing mapping under key: " + aRddName);
        addToNamedRdds(aRddName, mappingRdd);
    }

    /**
     * @param aMappingRDD
     * @param aColumnName
     * @return the number of distinct values for the given column index (as computed by the nominal to number value
     *         mapping above)
     */
    public static long getNumberValuesOfColumn(final JavaRDD<Row> aMappingRDD, final String aColumnName) {
        final long count = aMappingRDD.filter(new Function<Row, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(final Row aRow) throws Exception {
                return aRow.getString(0).equals(aColumnName);
            }
        }).count();

        if (count == 1) {
            //binary mapping, we store only one mapping, but there are 2 values
            return 2;
        }
        return count;
    }

    /**
     * extract Map storing arity of categorical features from the mapping RDD
     *
     * @param aColNames list of columns to be used
     * @param aMappingRDD
     * @return Map storing arity of categorical features. E.g., an entry (n -> k) indicates that feature n is
     *         categorical with k categories indexed from 0: {0, 1, ..., k-1}.
     */
    public static Map<Integer, Integer> extractNominalFeatureInfo(final List<String> aColNames,
        final JavaRDD<Row> aMappingRDD) {
        final Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        int ix = 0;
        for (String colNames : aColNames) {
            //note that 'colIx' is the index of the numeric column, but getNumberValuesOfColumn requires
            // the index of the original nominal column
            final Long numValues = ConvertNominalValuesJob.getNumberValuesOfColumn(aMappingRDD, colNames);
            //note that 'colIx' is the index of the numeric column, but the DT learner requires the index in the vector
            if (numValues > 0) {
                //if there is no entry then we assume that it is a true numeric feature
                categoricalFeaturesInfo.put(ix, numValues.intValue());
            }
            ix++;
        }
        return categoricalFeaturesInfo;
    }

}
