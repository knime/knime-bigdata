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

import java.util.List;
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
import com.knime.bigdata.spark.jobserver.server.NominalValueMapping;
import com.knime.bigdata.spark.jobserver.server.NominalValueMappingFactory;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * converts nominal values from a set of columns to numbers and adds corresponding new columns
 *
 * @author dwk
 */
public class ApplyNominalValueMappingJob extends AbstractStringMapperJob {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_MAPPING_TABLE = ConvertNominalValuesJob.PARAM_RESULT_MAPPING;

    private final static Logger LOGGER = Logger.getLogger(ApplyNominalValueMappingJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = super.validateParam(aConfig);

        if (msg == null && !aConfig.hasInputParameter(PARAM_MAPPING_TABLE)) {
            msg = "Input parameter '" + PARAM_MAPPING_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    @Override
    void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        super.validateInput(aConfig);
        String msg = null;
        final String key = aConfig.getInputParameter(PARAM_MAPPING_TABLE);
        if (key == null) {
            msg = "Input parameter at port 2 is missing!";
        } else if (!validateNamedRdd(key)) {
            msg = "Mapping table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ": " + msg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    JobResult execute(final SparkContext aContext, final JobConfig aConfig, final JavaRDD<Row> aInputRdd,
        final int[] aColIds, final Map<Integer, String> aColNameForIndex) throws GenericKnimeSparkException {
        // construct NominalValueMapping from mapping RDD
        final List<Row> mappingsTable = getFromNamedRdds(aConfig.getInputParameter(PARAM_MAPPING_TABLE)).collect();
        NominalValueMapping mappings = NominalValueMappingFactory.fromTable(mappingsTable);

        // apply mapping
        JavaRDD<Row> mappedData = RDDUtilsInJava.applyLabelMapping(aInputRdd, aColIds, mappings);

        // store result in named RDD
        LOGGER.log(Level.INFO, "Storing mapped data under key: " + aConfig.getOutputStringParameter(PARAM_RESULT_TABLE));
        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), mappedData);

        final MappedRDDContainer mappedDataContainer = new MappedRDDContainer(mappedData, mappings);
        //number of all (!)  columns in input data table
        final int offset = aInputRdd.take(1).get(0).length();
        mappedDataContainer.createMappingTable(aColNameForIndex, offset);
        LOGGER.log(Level.INFO, "done");
        return JobResult.emptyJobResult().withMessage("OK").withObjectResult(mappedDataContainer);
    }

}
