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
 * ---------------------------------------------------------------------
 *
 * History
 *   Nov 21, 2017 (Tobias): created
 */
package org.knime.bigdata.commons;

import org.knime.core.eclipseUtil.GlobalClassCreator;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeFactoryClassMapper;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;

/**
 * {@link NodeFactoryClassMapper} implementation that maps from com.knime.bigdata packages to org.knime.bigdata
 * NodeFactory implementations
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class BigDataNodeFactoryClassMapper extends NodeFactoryClassMapper {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BigDataNodeFactoryClassMapper.class);

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public NodeFactory<NodeModel> mapFactoryClassName(final String factoryClassName) {
        if (factoryClassName.startsWith("com.knime.bigdata")) {
            final String newClassName = factoryClassName.replace("com.knime.bigdata", "org.knime.bigdata");
            try {
                return (NodeFactory<NodeModel>)((GlobalClassCreator.createClass(newClassName)).newInstance());
            } catch (Exception e) {
                //this might happen for old com.knime.bigdata classes
                LOGGER.debug("Could not find class " + newClassName + " for " + factoryClassName);
            }
        }
        return null;
    }

}
