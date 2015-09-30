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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.MappedRDDContainer;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
public class ConvertNominalValuesJob extends AbstractStringMapperJob {

    private static final long serialVersionUID = 1L;

    /**
     * type of mapping
     */
    public static final String PARAM_MAPPING_TYPE = "MappingType";

    /**
     * keep original columns or not, default is true
     */
    public static final String PARAM_KEEP_ORIGINAL_COLUMNS = "keepOrigCols";

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

        final boolean keepOriginalColumns;
        if (aConfig.hasInputParameter(PARAM_KEEP_ORIGINAL_COLUMNS)) {
            keepOriginalColumns = aConfig.getInputParameter(PARAM_KEEP_ORIGINAL_COLUMNS, Boolean.class);
        } else {
            keepOriginalColumns = true;
        }

        //use only the column indices when converting
        final MappedRDDContainer mappedData =
            RDDUtilsInJava.convertNominalValuesForSelectedIndices(aRowRDD, aColIds, mappingType, keepOriginalColumns);

        LOGGER
            .log(Level.INFO, "Storing mapped data under key: " + aConfig.getOutputStringParameter(PARAM_RESULT_TABLE));

        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), mappedData.m_RddWithConvertedValues);

        //number of all (!)  columns in input data table
        int offset = aRowRDD.take(1).get(0).length();
        mappedData.createMappingTable(aColNameForIndex, offset);
        return JobResult.emptyJobResult().withMessage("OK").withObjectResult(mappedData);
    }
}
