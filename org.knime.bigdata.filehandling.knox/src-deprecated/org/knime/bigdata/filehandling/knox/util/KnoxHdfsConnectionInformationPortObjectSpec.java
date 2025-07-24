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
 *   Nov 11, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.filehandling.knox.util;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.bigdata.filehandling.knox.KnoxHDFSConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;

/**
 * A {@link ConnectionInformationPortObjectSpec} for Web HDFS connection to a KNOX instance.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@Deprecated
public class KnoxHdfsConnectionInformationPortObjectSpec extends ConnectionInformationPortObjectSpec {

    /**
     * @noreference This class is not intended to be referenced by clients.
     * @deprecated
     */
    @Deprecated
    public static final class Serializer
        extends AbstractSimplePortObjectSpecSerializer<KnoxHdfsConnectionInformationPortObjectSpec> { }

    private KnoxHDFSConnectionInformation m_connectionInformation;

    /**
     * Empty constructor.
     * @deprecated
     */
    @Deprecated
    public KnoxHdfsConnectionInformationPortObjectSpec() {
    }

    /**
     * Creates a {@link ConnectionInformationPortObjectSpec}.
     *
     * @param connectionInformation {@link KnoxHDFSConnectionInformation} used for the connection.
     * @deprecated
     */
    @Deprecated
    public KnoxHdfsConnectionInformationPortObjectSpec(
        final KnoxHDFSConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;
    }

    /**
     * Return the connection information contained by this port object spec.
     *
     * @return The content of this port object
     */
    @Override
    public ConnectionInformation getConnectionInformation() {
        return m_connectionInformation;
    }

    @Override
    public boolean equals(final Object ospec) {
        if (ospec == this) {
            return true;
        }
        if (!(ospec instanceof KnoxHdfsConnectionInformationPortObjectSpec)) {
            return false;
        }
        return super.equals(ospec);
    }

    @Override
    public int hashCode() {
        return m_connectionInformation == null ? 0 : m_connectionInformation.hashCode();
    }

    @Override
    protected void save(final ModelContentWO model) {
        m_connectionInformation.save(model);
    }

    @Override
    protected void load(final ModelContentRO model) throws InvalidSettingsException {
        m_connectionInformation = KnoxHDFSConnectionInformation.load(model);
    }
}
