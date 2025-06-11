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
 *   2025-05-22 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.delta.types;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import io.delta.kernel.types.TimestampNTZType;

/**
 * Value reader of {@link TimestampNTZType}.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public class DeltaTableTimestampNTZValue extends DeltaTableValue {

    DeltaTableTimestampNTZValue(final DeltaTableRandomAccessibleRow randomAccessibleRow, final int ordinal) {
        super(randomAccessibleRow, ordinal);
    }

    private Instant getInstant() {
        final var micros = m_randomAccessibleRow.getRow().getLong(m_ordinal);
        final var seconds = TimeUnit.SECONDS.convert(micros, TimeUnit.MICROSECONDS);
        final var nanos = (micros % (1000 * 1000L)) * 1000L;
        return Instant.ofEpochSecond(seconds, nanos);
    }

    @Override
    public <T> T getObj(final Class<T> expectedClass) {
        if (isNull()) {
            return null;
        } else if (expectedClass == String.class) {
            return expectedClass.cast(getString());
        } else if (expectedClass == LocalDate.class) {
            final var localDate = LocalDate.ofInstant(getInstant(), ZoneOffset.UTC).atStartOfDay();
            return expectedClass.cast(localDate);
        } else {
            final var localDateTime = LocalDateTime.ofInstant(getInstant(), ZoneOffset.UTC);
            return expectedClass.cast(localDateTime);
        }
    }

    @Override
    public String getString() {
        return isNull() ? "null" : getObj(LocalDateTime.class).toString();
    }

}
