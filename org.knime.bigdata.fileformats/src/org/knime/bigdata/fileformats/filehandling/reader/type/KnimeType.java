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
 *   Nov 5, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.fileformats.filehandling.reader.type;

import java.util.Set;

import org.knime.core.data.DataType;

/**
 * Interface for types supported in KNIME.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface KnimeType {

    /**
     * Indicates whether this is a {@link ListKnimeType}.
     *
     * @return {@code true} if this is a {@link ListKnimeType}
     */
    boolean isList();

    /**
     * Convenience method for accessing this instance as a {@link ListKnimeType} provided it is one.
     *
     * @return this instance as a {@link ListKnimeType} provided it is one
     */
    ListKnimeType asListType();

    /**
     * Convenience method for accessing this instance as a {@link PrimitiveKnimeType} provided it is one. Should only be
     * called if {@link #isList()} returned {@code false}.
     *
     * @return this instance as a {@link PrimitiveKnimeType} provided it is one
     */
    PrimitiveKnimeType asPrimitiveType();

    /**
     * Returns the default KNIME {@link DataType} for this type.
     *
     * @return the default KNIME {@link DataType} for this type
     */
    DataType getDefaultDataType();

    /**
     * Provides the {@link Set} of {@link Class classes} this KnimeType can be converted to.
     *
     * @return the java classes this type can be converted to
     */
    Set<Class<?>> getSupportedJavaClasses();

}