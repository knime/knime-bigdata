/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Nov 19, 2017 (Tobias): created
 */
package org.knime.bigdata.commons.config;

import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.knime.core.node.NodeLogger;

/**
 * Class with helper methods to work with the Eclipse preferences such as copying old preferences into new preferences.
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class EclipsePreferencesHelper {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(EclipsePreferencesHelper.class);

    /**
     * Copies all settings from the legacy preference file (starting with com.knime.) into a new preference file
     * (starting with org.knime.) and also converts all com.knime. to org.knime keys.
     * Also looks for new preferences that have been added using the import preferences function on eclipse.
     * Imported com.knime preferences are overwriting the corresponding org.knime preferences after a restart of
     * KNIME.
     * @param orgPreferenceName the preference name e.g. symbolic name of the bundle of the preference store
     */
    public static void checkForLegacyPreferences(final String orgPreferenceName) {
        final String comPreferenceName = orgPreferenceName.replace("org.knime.", "com.knime.");
        try {
            //we have to always look for the legacy com.knime preferences in case old preferences have been imported
            final IEclipsePreferences comPreferences = InstanceScope.INSTANCE.getNode(comPreferenceName);
            final String[] comKeys = comPreferences.keys();
            if (comKeys != null && comKeys.length > 0) {
                //copy all old to new preferences. This is also the case after importing old com.knime preferences
                IEclipsePreferences orgPreferences = InstanceScope.INSTANCE.getNode(orgPreferenceName);
                for (String comKey : comKeys) {
                    final String orgKey = comKey.replace("com.knime.", "org.knime.");
                    orgPreferences.put(orgKey, comPreferences.get(comKey, null));
                }
                orgPreferences.flush();
                //delete the old com.knime preferences after importing them
                comPreferences.clear();
                comPreferences.flush();
                comPreferences.removeNode();
            }
        } catch (Exception e) {
            LOGGER.info("Exception copying com.knime preferences from" + comPreferenceName
                + " to " + orgPreferenceName + ". Exception: " + e.getMessage(), e);
        }
    }
}
