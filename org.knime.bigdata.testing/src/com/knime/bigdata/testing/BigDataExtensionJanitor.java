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
 *   Nov 7, 2016 (Sascha Wolke, KNIME.com): created
 */
package com.knime.bigdata.testing;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.knime.bigdata.testing.FlowVariableReader;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.testing.core.TestrunJanitor;

/**
 * Provides big data flow variables based on a CSV file. CSV contains string key/value pairs and a header line.
 *
 * Additional, KRB5_CONFIG can be set in ENV and system property java.security.krb5.conf will be provided.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class BigDataExtensionJanitor extends TestrunJanitor {
    private final static NodeLogger LOGGER = NodeLogger.getLogger(BigDataExtensionJanitor.class);

    private final List<FlowVariable> m_flowVariables = new ArrayList<>();

    /** Default constructor. */
    public BigDataExtensionJanitor() {
        final String kerberosConfig = System.getenv("KRB5_CONFIG");
        if (kerberosConfig != null && !System.getProperties().contains("java.security.krb5.conf")) {
            LOGGER.info("Setting system property java.security.krb5.conf = " + kerberosConfig);
            System.setProperty("java.security.krb5.conf", kerberosConfig);
        }
    }

    @Override
    public synchronized List<FlowVariable> getFlowVariables() {
        return m_flowVariables;
    }

    @Override
    public synchronized void before() throws Exception {
        m_flowVariables.clear();
        m_flowVariables.addAll(FlowVariableReader.readFromCsv().values());
        m_flowVariables.sort(new Comparator<FlowVariable>() {
            @Override
            public int compare(FlowVariable a, FlowVariable b) {
                return b.getName().compareTo(a.getName());
            }
        });
    }

    @Override
    public void after() throws Exception {
        //nothing to do
    }

    @Override
    public String getName() {
        return "Big Data Extension flowvariables.csv ";
    }

    @Override
    public String getID() {
        return "com.knime.bigdata.testing.BigDataExtensionJanitor";
    }

    @Override
    public String getDescription() {
        return "Loads flowvariables.csv from path provided by environment FLOWVARS variable, relative to current workflow or in root workspace directory.";
    }
}
