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
 *   Created on 29.05.2015 by koetter
 */
package com.knime.bigdata.spark.core.port.data;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class AbstractSparkRDD implements SparkRDD {

    private static final String SPARK_DATA = "data";

    private static final String KEY_TABLE_NAME = "tableName";

    /**
     * Key required to load legacy workflows (KNIME Spark Executor <= v1.3)
     */
    private static final String KEY_CONTEXT_LEGACY = "context";

    /**
     * Key required to load current workflows (KNIME Spark Executor >v1.3)
     */
    private static final String KEY_CONTEXT_ID = "contextID";


    private final String m_id;
    private final SparkContextID m_contextID;

    /**
     * @param id the unique id of the Spark RDD
     * @param context the Spark context the RDD lives in
     *
     */
    protected AbstractSparkRDD(final SparkContextID context, final String id) {
        if (context == null) {
            throw new NullPointerException("context must not be null");
        }
        if (id == null) {
            throw new NullPointerException("tableName must not be null");
        }
        m_contextID = context;
        m_id = id;
    }

    /**
     * @param in
     * @throws IOException
     */
    protected AbstractSparkRDD(final ZipInputStream in) throws IOException {
        try {
            final ZipEntry ze = in.getNextEntry();
            if (!ze.getName().equals(SPARK_DATA)) {
                throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                    + SPARK_DATA + "\".");
            }
            final ModelContentRO sparkModel = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));

            if (sparkModel.containsKey(KEY_CONTEXT_ID)) {
                // Load current workflow (KNIME Spark Executor >v1.3)
                m_contextID = SparkContextID.fromConfigRO(sparkModel.getConfig(KEY_CONTEXT_ID));
            } else if (sparkModel.containsKey(KEY_CONTEXT_LEGACY)) {
                // Load legacy workflow (KNIME Spark Executor <= v1.3)
                m_contextID = SparkContextConfig.createSparkContextIDFromLegacyConfig(sparkModel.getConfig(KEY_CONTEXT_LEGACY));
            } else {
                throw new IOException(String.format("Did not find one of the expected keys \"%s\" and \"%s\"", KEY_CONTEXT_LEGACY, KEY_CONTEXT_ID));
            }
            m_id = sparkModel.getString(KEY_TABLE_NAME);
        } catch (InvalidSettingsException ise) {
            throw new IOException(ise);
        }
    }

    /**
     * @param out
     * @throws IOException
     */
    protected void save(final ZipOutputStream out) throws IOException {
        final ModelContent sparkModel = new ModelContent(SPARK_DATA);
        m_contextID.saveToConfigWO(sparkModel.addConfig(KEY_CONTEXT_ID));
        sparkModel.addString(KEY_TABLE_NAME, m_id);
        out.putNextEntry(new ZipEntry(SPARK_DATA));
        sparkModel.saveToXML(new NonClosableOutputStream.Zip(out));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getID() {
        return m_id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextID getContextID() {
        return m_contextID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean compatible(final SparkRDD outputRDD) {
        return m_contextID.equals(outputRDD.getContextID());
    }

}