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
 *   Created on 23.08.2015 by koetter
 */
package com.knime.bigdata.spark.core.node;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.dmg.pmml.DerivedFieldDocument.DerivedField;
import org.dmg.pmml.InlineTableDocument.InlineTable;
import org.dmg.pmml.MapValuesDocument.MapValues;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.pmml.PMMLPortObject;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;

import com.knime.bigdata.spark.core.job.util.MLSettings;
import com.knime.bigdata.spark.core.job.util.MLlibSettings;
import com.knime.bigdata.spark.core.job.util.NominalFeatureInfo;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.core.util.SparkUtil;

/**
 * Settings class that contains commonly used settings required to learn a Spark MLlib model such as
 * class column and feature columns. It also provides helper method to be used in the NodeModel and NodeDialog.
 * @author Tobias Koetter, KNIME.com
 * @author Ole Ostergaard, KNIME.com
 */
public class MLNodeSettings {
    private final SettingsModelString m_classColModel = new SettingsModelString("classColumn", null);


    private final SettingsModelColumnFilter2 m_featureColsModel;

    private boolean m_requiresClassCol;

    /**
     * @param requiresClassCol <code>true</code> if the class column name is mandatory
     *
     */
    public MLNodeSettings(final boolean requiresClassCol) {
        this(requiresClassCol, false);
    }

    /**
     * @param requiresClassCol <code>true</code> if the class column name is mandatory
     *
     */
    @SuppressWarnings("unchecked")
    public MLNodeSettings(final boolean requiresClassCol, final boolean allowsNominalFeatures) {
        m_requiresClassCol = requiresClassCol;
        if (allowsNominalFeatures) {
            m_featureColsModel = new SettingsModelColumnFilter2("featureColumns", DoubleValue.class, StringValue.class, BooleanValue.class);
        } else {
            m_featureColsModel = new SettingsModelColumnFilter2("featureColumns", DoubleValue.class);
        }
    }

    /**
     * @return the class column name
     */
    public String getClassCol() {
        return m_classColModel.getStringValue();
    }

    /**
     * @param spec the {@link DataTableSpec} to get the feature columns for
     * @return the selected feature column names
     */
    public String[] getFeatureCols(final DataTableSpec spec) {
        return m_featureColsModel.applyTo(spec).getIncludes();
    }

    /**
     * @return the classColModel
     */
    public SettingsModelString getClassColModel() {
        return m_classColModel;
    }

    /**
     * @return the featureColsModel
     */
    public SettingsModelColumnFilter2 getFeatureColsModel() {
        return m_featureColsModel;
    }

    /**
     * @return the requiresClassCol
     */
    public boolean isRequiresClassCol() {
        return m_requiresClassCol;
    }

    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @throws InvalidSettingsException  if the settings are invalid
     */
    public void check(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final String classColName = m_classColModel.getStringValue();
        if (m_requiresClassCol && classColName == null) {
            throw new InvalidSettingsException("No class column selected");
        }
        final int classColIdx = tableSpec.findColumnIndex(classColName);
        if (m_requiresClassCol && classColIdx < 0) {
            throw new InvalidSettingsException("Class column :" + classColName + " not found in input data");
        }
        final FilterResult result = m_featureColsModel.applyTo(tableSpec);
        final List<String> featureColNames =  Arrays.asList(result.getIncludes());
        Integer[] featureColIdxs = SparkUtil.getColumnIndices(tableSpec, featureColNames);
        if (Arrays.asList(featureColIdxs).contains(Integer.valueOf(classColIdx))) {
            throw new InvalidSettingsException("Class column '" + classColName + "' also selected as feature column");
        }
    }

    /**
     * @param settings the {@link NodeSettingsWO} to write to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_classColModel.saveSettingsTo(settings);
        m_featureColsModel.saveSettingsTo(settings);
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException  if the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (m_requiresClassCol) {
            m_classColModel.validateSettings(settings);
            final String classCol =
                    ((SettingsModelString)m_classColModel.createCloneWithValidatedValue(settings)).getStringValue();
            if (classCol == null || classCol.isEmpty()) {
                throw new InvalidSettingsException("Class column must not be empty");
            }
        }
        m_featureColsModel.validateSettings(settings);
    }

    /**
     * @param settings the {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (m_requiresClassCol) {
            m_classColModel.loadSettingsFrom(settings);
        }
        m_featureColsModel.loadSettingsFrom(settings);
    }

    /**
     * @param rdd the {@link SparkDataPortObject}
     * @return the {@link MLlibSettings} object for the input table spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    public MLSettings getSettings(final SparkDataPortObject rdd) throws InvalidSettingsException {
        return getSettings(rdd.getTableSpec(), null);
    }
    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @return the {@link MLlibSettings} object for the input table spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    public MLSettings getSettings(final DataTableSpec tableSpec) throws InvalidSettingsException {
        return getSettings(tableSpec, null);
    }


    /**
     * @param rdd {@link SparkDataPortObject}
     * @param mapping {@link PMMLPortObject} with the category to number mapping info or <code>null</code> if
     * not available
     * @return the {@link MLlibSettings} object for the input table spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    public MLSettings getSettings(final SparkDataPortObject rdd, final PMMLPortObject mapping)
            throws InvalidSettingsException {
        return getSettings(rdd.getTableSpec(), mapping);
    }
    /**
     * @param tableSpec the original input {@link DataTableSpec}
     * @param mapping {@link PMMLPortObject} with the category to number mapping info or <code>null</code> if
     * not available
     * @return the {@link MLlibSettings} object for the input table spec
     * @throws InvalidSettingsException if the settings are invalid
     */
    public MLSettings getSettings(final DataTableSpec tableSpec, final PMMLPortObject mapping) throws InvalidSettingsException {
        final String classColName = getClassCol();
        final int classColIdx = tableSpec.findColumnIndex(classColName);
        if (m_requiresClassCol && classColIdx < 0) {
            throw new InvalidSettingsException("Class column :" + classColName + " not found in input data");
        }
        final List<String> featureColNames = Arrays.asList(getFeatureCols(tableSpec));
        final Integer[] featureColIdxs = SparkUtil.getColumnIndices(tableSpec, featureColNames);
        if (Arrays.asList(featureColIdxs).contains(Integer.valueOf(classColIdx))) {
            throw new InvalidSettingsException("Class column also selected as feature column");
        }

        //PMML mapping info if available
        final Long numberOfClasses;
        final NominalFeatureInfo featureInfo;
        if (mapping != null) {
            final Map<String, DerivedField> mapValues = MLNodeSettings.getMapValues(mapping);
             numberOfClasses = MLNodeSettings.getNoOfClassVals(mapValues, classColName);
             featureInfo = MLNodeSettings.getNominalFeatureInfo(featureColNames, mapValues);
        } else {
            numberOfClasses = null;
            featureInfo = null;
        }

        return new MLSettings(tableSpec, classColName, classColIdx, numberOfClasses, featureColNames,
            featureColIdxs, featureInfo);
    }

    /**
     * @param model the PMML model
     * @return the field in the first FieldColumnPair of the MapValues mapped
     * to the MapValues Model
     */
    public static Map<String, DerivedField> getMapValues(final PMMLPortObject model) {
        final Map<String, DerivedField> mapValues = new LinkedHashMap<String, DerivedField>();
        final DerivedField[] derivedFields = model.getDerivedFields();
        for (final DerivedField derivedField : derivedFields) {
            final MapValues map = derivedField.getMapValues();
            if (null != map) {
                // This is the field name the mapValues is based on
                String name = derivedField.getDisplayName();
                if (name == null) {
                    name = derivedField.getName();
                }
                mapValues.put(name, derivedField);
            }
        }
        return mapValues;
    }

    /**
     * @param featureColNames the names of the feature columns in the same order as in the Spark RDD
     * @param mapValues the derived fields from the PMML
     * @return the nominal column indices as first element and the number of unique values of the corresponding
     * column as second argument
     */
    public static NominalFeatureInfo getNominalFeatureInfo(final List<String> featureColNames,
        final Map<String, DerivedField> mapValues) {
        final NominalFeatureInfo nominalFeatureInfo = new NominalFeatureInfo();
        int idx = 0;
        for (final String col : featureColNames) {
            final DerivedField derivedField = mapValues.get(col);
            if (derivedField != null) {
                final MapValues map = derivedField.getMapValues();
                final InlineTable table = map.getInlineTable();
                final int noOfVals = table.sizeOfRowArray();
                nominalFeatureInfo.add(Integer.valueOf(idx), noOfVals);
            }
            idx++;
        }
        return nominalFeatureInfo;
    }

    /**
     * @param mapValues the {@link DerivedField}
     * @param classColName the name of the class column
     * @return the number of unique classes if the class column is one of the {@link DerivedField}s
     */
    public static Long getNoOfClassVals(final Map<String, DerivedField> mapValues, final String classColName) {
        if (mapValues.containsKey(classColName)) {
            final DerivedField derivedField = mapValues.get(classColName);
            final MapValues map = derivedField.getMapValues();
            final InlineTable table = map.getInlineTable();
            final int noOfVals = table.sizeOfRowArray();
            return Long.valueOf(noOfVals);
        }
        return null;
    }

    /**
     * @param portIdx the index of the {@link SparkDataPortObjectSpec}
     * @param specs the {@link PortObjectSpec} array
     * @return the {@link DataTableSpec}[] that can be used in the loadSettingsFrom methods of the
     * {@link DialogComponent}s
     * @throws NotConfigurableException if the specs are not valid
     */
    public static DataTableSpec[] getTableSpecInDialog(final int portIdx, final PortObjectSpec[] specs)
            throws NotConfigurableException {
        if (specs == null || specs.length <= portIdx || specs[portIdx] == null) {
            throw new NotConfigurableException("No input connection available");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)specs[portIdx];
        final DataTableSpec[] tableSpecs = new DataTableSpec[] {spec.getTableSpec()};
        return tableSpecs;
    }
}
