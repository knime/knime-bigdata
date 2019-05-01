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
 *   08.04.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.hive;

import org.knime.bigdata.database.hive.agent.HiveCaseSupportedBinner;
import org.knime.bigdata.database.hive.agent.HiveSampling;
import org.knime.database.agent.AbstractDBAgentFactory;
import org.knime.database.agent.DBAgentFactory;
import org.knime.database.agent.binning.DBBinner;
import org.knime.database.agent.sampling.DBSampling;
import org.knime.database.agent.sampling.impl.DefaultDBSampling;
import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;

/**
 * {@linkplain DBAgentFactory Agent factory} for Hive.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class HiveAgentFactory extends AbstractDBAgentFactory {


    private static final AttributeCollection SAMPLING_ATTRIBUTES;

    static {
        final AttributeCollection.Builder builder = AttributeCollection.builder(DefaultDBSampling.ATTRIBUTES);
        builder.add(Accessibility.HIDDEN, DefaultDBSampling.ATTRIBUTE_CAPABILITY_RANDOM, true);
        builder.add(Accessibility.HIDDEN, DefaultDBSampling.ATTRIBUTE_CAPABILITY_RANDOM_SEED, true);
        SAMPLING_ATTRIBUTES = builder.build();
    }

    /**
    * Constructs a {@link HiveAgentFactory}.
    */
   public HiveAgentFactory() {
        putAttributes(DBSampling.class, SAMPLING_ATTRIBUTES);
        putCreator(DBSampling.class, parameters -> new HiveSampling(parameters.getSession()));
        putCreator(DBBinner.class, parameters -> {
            return new HiveCaseSupportedBinner(parameters.getSession());
        });
   }

}
