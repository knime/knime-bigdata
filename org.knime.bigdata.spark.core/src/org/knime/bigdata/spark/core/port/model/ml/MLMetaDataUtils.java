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
 *   Created on Jun 1, 2019 by bjoern
 */
package org.knime.bigdata.spark.core.port.model.ml;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class MLMetaDataUtils {

    private static final String KEY_NOMINAL_FEATURE_MAPPINGS = "nominalFeatureMappings";

    private static final String KEY_NOMINAL_TARGET_MAPPINGS = "nominalTargetMappings";

    private static final String KEY_ADDITIONAL_VALUES = "additionalValues";

    public static void saveToModelContent(final MLMetaData metaData, final ModelContentWO modelContent) {

        final Map<Integer, List<String>> nominalFeatureMappings = metaData.getNominalFeatureValueMappings();
        if (nominalFeatureMappings != null) {
            final ModelContentWO nominalFeatureMappingsContent =
                modelContent.addModelContent(KEY_NOMINAL_FEATURE_MAPPINGS);
            for (Entry<Integer, List<String>> entry : nominalFeatureMappings.entrySet()) {
                nominalFeatureMappingsContent.addStringArray(Integer.toString(entry.getKey()),
                    entry.getValue().toArray(new String[0]));
            }
        }

        final List<String> nominalTargetMappings = metaData.getNominalTargetValueMappings();
        if (nominalTargetMappings != null) {
            modelContent.addStringArray(KEY_NOMINAL_TARGET_MAPPINGS, nominalTargetMappings.toArray(new String[0]));
        }

        final ModelContentWO additionalValuesContent = modelContent.addModelContent(KEY_ADDITIONAL_VALUES);
        for (Entry<String, Object> entry : metaData.getAdditionalValues().entrySet()) {
            final Object value = entry.getValue();
            if (value instanceof Integer) {
                additionalValuesContent.addInt(entry.getKey(), (int)value);
                additionalValuesContent.addString(entry.getKey() + ".type", "int");
            } else if (value instanceof Double) {
                additionalValuesContent.addDouble(entry.getKey(), (double)value);
                additionalValuesContent.addString(entry.getKey() + ".type", "double");
            } else if (value instanceof String) {
                additionalValuesContent.addString(entry.getKey(), (String)value);
                additionalValuesContent.addString(entry.getKey() + ".type", "string");
            }
        }
    }

    public static MLMetaData loadFromModelContent(final ModelContentRO modelContent) throws InvalidSettingsException {

        MLMetaData toReturn = new MLMetaData();
        if (modelContent.containsKey(KEY_NOMINAL_FEATURE_MAPPINGS)) {
            final ModelContentRO nominalFeatureMappingsContent =
                modelContent.getModelContent(KEY_NOMINAL_FEATURE_MAPPINGS);
            for (String key : nominalFeatureMappingsContent.keySet()) {
                toReturn.withNominalFeatureValueMapping(Integer.parseInt(key),
                    Arrays.asList(nominalFeatureMappingsContent.getStringArray(key)));
            }
        }

        if (modelContent.containsKey(KEY_NOMINAL_TARGET_MAPPINGS)) {
            toReturn.withNominalTargetValueMappings(
                Arrays.asList(modelContent.getStringArray(KEY_NOMINAL_TARGET_MAPPINGS)));
        }

        final ModelContentRO additionalValuesContent = modelContent.getModelContent(KEY_ADDITIONAL_VALUES);
        for (String key : additionalValuesContent.keySet()) {
            final String type = additionalValuesContent.getString(key + ".type");
            if (type.equals("int")) {
                toReturn.setInteger(key, additionalValuesContent.getInt(key));
            } else if (type.equals("double")) {
                toReturn.setDouble(key, additionalValuesContent.getDouble(key));
            } else if (type.equals("string")) {
                toReturn.setString(key, additionalValuesContent.getString(key));
            }
        }

        return toReturn;
    }
}
