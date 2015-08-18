/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   25.11.2011 (hofer): created
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJavaSnippet extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger("KNIME Spark Java Snippet");

    /**
     * second input table name
     */
    public static final String PARAM_INPUT_TABLE_KEY2 = "SecondInputTable";

    private static final String PARAM_MODEL = ParameterConstants.PARAM_MODEL_NAME;

    private static final String PARAM_MAIN_CLASS = ParameterConstants.PARAM_MAIN_CLASS;

    /**
     * parse parameters
     */
    @Override
    public final SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }
        if (msg == null && !aConfig.hasInputParameter(PARAM_MODEL)) {
            msg = "Compiled snippet class missing!";
        }
        if (msg == null && !aConfig.hasInputParameter(PARAM_MAIN_CLASS)) {
            msg = "Main class name missing!";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        if (aConfig.hasInputParameter(PARAM_INPUT_TABLE)
            && !validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE))) {
            msg = "(First) Input data table missing for key: " + aConfig.getInputParameter(PARAM_INPUT_TABLE);
        }
        if (aConfig.hasInputParameter(PARAM_INPUT_TABLE_KEY2) && !validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY2))) {
            msg = "Second input data table missing for key: " + aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY2);
        }

        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client the primary result of this job should be a side
     * effect - new new RDD in the map of named RDDs
     *
     * @return JobResult with table information
     */
    @Override
    protected final JobResult runJobWithContext(final SparkContext aSparkContext, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        validateInput(aConfig);
        try {
            final JavaRDD<Row> rowRDD1;
            if (aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
                rowRDD1 = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
            } else {
                rowRDD1 = null;
            }
            final JavaRDD<Row> rowRDD2;
            if (aConfig.hasInputParameter(PARAM_INPUT_TABLE_KEY2)) {
                rowRDD2 = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE_KEY2));
            } else {
                rowRDD2 = null;
            }
            final String mainClass = aConfig.getInputParameter(PARAM_MAIN_CLASS);
            System.out.println("Main class: " + mainClass);
            final Map<String, byte[]> bytecode = aConfig.decodeFromInputParameter(PARAM_MODEL);
            final ClassLoader cl = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
                /** {@inheritDoc} */
                @Override
                protected Class<?> findClass(final String name) throws ClassNotFoundException {
                    System.out.println("In class loader: " + name);
                    byte[] bc = bytecode.get(name);
                    return defineClass(name, bc, 0, bc.length);
                }
            };
            System.out.println("Loading class:" + mainClass);
            final Class<?> modelClass = cl.loadClass(mainClass);
            System.out.println("Class loaded: " + modelClass);
            final Object instance = modelClass.newInstance();
            System.out.println("Object create: " + instance);
            Method applyMethod = modelClass.getMethod("apply", JavaSparkContext.class, JavaRDD.class, JavaRDD.class);
            System.out.println("Apply method loaded");
            LOGGER.log(Level.FINE, "starting transformation job...");
            System.out.println("Call apply method");
//            final JavaRDD<Row> resultRDD = apply(new JavaSparkContext(aSparkContext), rowRDD1, rowRDD2);
            final JavaRDD<Row> resultRDD =
                    (JavaRDD<Row>) applyMethod.invoke(instance, new JavaSparkContext(aSparkContext), rowRDD1, rowRDD2);
            System.out.println("AbstractSparkJavaSnippet Apply resultRDD: " + resultRDD);
            LOGGER.log(Level.FINE, "transformation completed");
            try {
                if (resultRDD != null) {
                    System.out.println("Getting schema for result");
                    addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), resultRDD);
//                    Method schemaMethod = modelClass.getMethod("getSchema", JavaRDD.class);
//                    System.out.println("Schema method loaded");
//                    System.out.println("Call schema method");
//                    final StructType schema = (StructType) schemaMethod.invoke(instance, resultRDD);
////                    final StructType schema = getSchema(resultRDD);
//                    System.out.println("Schema: " + schema);
                    return JobResult.emptyJobResult().withMessage("OK")
                        .withTable(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), StructTypeBuilder.fromRows(resultRDD.take(10)).build());
                }
                return JobResult.emptyJobResult().withMessage("OK");
            } catch (Exception e) {
                throw new GenericKnimeSparkException(e);
            }
        } catch (Exception e) {
            throw new GenericKnimeSparkException(e);
        }
    }
}
