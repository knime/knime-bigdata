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
 *   Created on Feb 12, 2015 by knime
 */
package com.knime.bigdata.spark.port.data;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.swing.JComponent;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.workflow.DataTableSpecView;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkData {

    private static final String SPARK_DATA = "data";
    private static final String TABLE_SPEC = "spec";
    private static final String KEY_TABLE_NAME = "tableName";

    private static final String DATA_ENTRY = "Data";
    private DataTableSpec m_spec;
    private String m_tableName;

    /**
     */
    public SparkData(final String tableName, final DataTableSpec spec) {
        m_tableName = tableName;
        m_spec = spec;
    }

    /**
     * @param in
     */
    SparkData(final ZipInputStream in) throws IOException {
        ZipEntry ze = in.getNextEntry();
        if (!ze.getName().equals(SPARK_DATA)) {
            throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                + SPARK_DATA + "\".");
        }
        try {
        final ModelContentRO sparkModel = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
        m_tableName = sparkModel.getString(KEY_TABLE_NAME);
        ze = in.getNextEntry();
        if (!ze.getName().equals(TABLE_SPEC)) {
            throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
                + TABLE_SPEC + "\".");
        }
        final ModelContentRO specModel = ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
            m_spec = DataTableSpec.load(specModel);
        } catch (InvalidSettingsException ise) {
            throw new IOException(ise);
        }
    }

    /**
     * @param out
     * @throws IOException
     */
    void save(final ZipOutputStream out) throws IOException {
        final ModelContent sparkModel = new ModelContent(SPARK_DATA);
        sparkModel.addString(KEY_TABLE_NAME, m_tableName);
        out.putNextEntry(new ZipEntry(SPARK_DATA));
        sparkModel.saveToXML(new NonClosableOutputStream.Zip(out));
        final ModelContent specModel = new ModelContent(TABLE_SPEC);
        m_spec.save(specModel);
        out.putNextEntry(new ZipEntry(TABLE_SPEC));
        specModel.saveToXML(new NonClosableOutputStream.Zip(out));
    }


    /**
     * @return the tableSpec
     */
    public DataTableSpec getTableSpec() {
        return m_spec;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return m_tableName;
    }

    /**
     * @return
     */
    public JComponent[] getViews() {
        return new JComponent[]{new DataTableSpecView(getTableSpec()), new SparkDataView(this)};
    }
}
