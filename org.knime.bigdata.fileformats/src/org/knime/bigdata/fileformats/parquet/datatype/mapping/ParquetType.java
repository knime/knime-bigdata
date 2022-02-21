package org.knime.bigdata.fileformats.parquet.datatype.mapping;

import java.util.Objects;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * ParquetType for Parquet type mapping
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 * @author Sascha Wolke, KNIME GmbH
 */
public class ParquetType {

    private final PrimitiveTypeName m_primitive;
    private final LogicalTypeAnnotation m_logical;
    private final OriginalType m_original;
    private final ParquetType m_listElementType;

    /**
     * Constructs a logical Parquet type
     *
     * @param primitive the primitive Parquet type
     * @param logical the logical Parquet type annotation
     */
    public ParquetType(final PrimitiveTypeName primitive, final LogicalTypeAnnotation logical) {
        m_primitive = primitive;
        m_logical = logical;
        m_original = null;
        m_listElementType = null;
    }

    /**
     * Constructs a original Parquet type
     *
     * @param primitive the primitive Parquet type
     * @param original the original Parquet type
     */
    public ParquetType(final PrimitiveTypeName primitive, final OriginalType original) {
        m_primitive = primitive;
        m_logical = null;
        m_original = original;
        m_listElementType = null;
    }

    /**
     * Constructs a primitive Parquet type
     *
     * @param primitive the primitive Parquet type
     */
    public ParquetType(final PrimitiveTypeName primitive) {
        m_primitive = primitive;
        m_logical = null;
        m_original = null;
        m_listElementType = null;
    }

    /**
     * Constructs a list with given element Parquet type
     */
    private ParquetType(final ParquetType element) {
        m_primitive = null;
        m_logical = null;
        m_original = null;
        m_listElementType = element;
    }

    /**
     * Creates a list Parquet type.
     *
     * @param elementType the type of the elements
     * @return a ParquetType for lists
     */
    public static ParquetType createListType(final ParquetType elementType) {
        return new ParquetType(elementType);
    }

    @Override
    public String toString() {
        // Note that this method is used by the data type mapping framework and must create uniq results!
        if (m_listElementType != null) {
            return String.format("LIST[%s]", m_listElementType.toString());
        } else if (m_logical != null) {
            return toLogicalTypeString(m_primitive, m_logical);
        } else if (m_original != null) { // UTF8 (BINARY)
            return String.format("%s (%s)", m_original, m_primitive);
        } else {
            return String.format("%s", m_primitive);
        }
    }

    static String toLogicalTypeString(final PrimitiveType.PrimitiveTypeName primitive, final LogicalTypeAnnotation logical) { // NOSONAR not very complex
        if (logical instanceof StringLogicalTypeAnnotation) {
            return String.format("STRING (%s)", primitive);
        } else if (logical instanceof EnumLogicalTypeAnnotation) {
            return String.format("ENUM (%s)", primitive);
        } else if (logical instanceof JsonLogicalTypeAnnotation) {
            return String.format("JSON (%s)", primitive);
        } else if (logical instanceof UUIDLogicalTypeAnnotation) {
            return String.format("UUID (%s)", primitive);
        } else if (logical instanceof IntLogicalTypeAnnotation) {
            final IntLogicalTypeAnnotation intType = (IntLogicalTypeAnnotation) logical;
            return String.format("INTEGER (%d bit, %s, %s)", //
                intType.getBitWidth(), //
                (intType.isSigned() ? "signed" : "unsigned"), //
                primitive);
        } else if (logical instanceof DecimalLogicalTypeAnnotation) {
            final DecimalLogicalTypeAnnotation decimalType = (DecimalLogicalTypeAnnotation) logical;
            return String.format("DECIMAL (%d precision, %d scale, %s)", //
                decimalType.getPrecision(), //
                decimalType.getScale(), //
                primitive);
        } else if (logical instanceof DateLogicalTypeAnnotation) {
            return String.format("DATE (%s)", primitive);
        } else if (logical instanceof TimeLogicalTypeAnnotation) {
            final TimeLogicalTypeAnnotation timeType = (TimeLogicalTypeAnnotation) logical;
            return String.format("TIME (%s, %s, %s)", //
                (timeType.isAdjustedToUTC() ? "adjusted to UTC" : "not adjusted to UTC"),
                timeType.getUnit(),
                primitive);
        } else if (logical instanceof TimestampLogicalTypeAnnotation) {
            final TimestampLogicalTypeAnnotation timestampType = (TimestampLogicalTypeAnnotation) logical;
            return String.format("TIMESTAMP (%s, %s, %s)", //
                (timestampType.isAdjustedToUTC() ? "adjusted to UTC" : "not adjusted to UTC"),
                timestampType.getUnit(),
                primitive);
        } else {
            throw new IllegalArgumentException("Usupported logical type: " + logical);
        }
    }

    /**
     * Converts this type into an external logical type string.
     *
     * @return logical type as string
     */
    public String toExternalLogicalTypeString() {
        return constructParquetType("field").toString();
    }

    /**
     * Creates a ParquetType from the given external logical type string.
     *
     * @param string the string to parse
     * @return the resulting ParquetType
     */
    public static ParquetType fromExternalLogicalTypeString(final String string) {
        // wrap type string into a message and append a ";" if it is not a list group
        var message = String.format("message document { %s%s }", string, (string.endsWith("}") ? "" : ";"));
        var messageType = MessageTypeParser.parseMessageType(message);
        var type = messageType.getFields().get(0);
        if (type.isPrimitive()) {
            var primitive = type.asPrimitiveType();
            return new ParquetType(primitive.getPrimitiveTypeName(), primitive.getLogicalTypeAnnotation());
        } else { // list
            var primitive = type.asGroupType().getFields().get(0).asPrimitiveType();
            var element = new ParquetType(primitive.getPrimitiveTypeName(), primitive.getLogicalTypeAnnotation());
            return new ParquetType(element);
        }
    }

    /**
     * Converts this type into an external original type string.
     *
     * @return original type as string
     */
    public String toExternalOriginalTypeString() {
        if (m_listElementType != null) {
            return String.format("LIST[%s]", m_listElementType.toString());
        } else if (m_original != null) { // UTF8 (BINARY)
            return String.format("%s (%s)", m_original, m_primitive);
        } else {
            return String.format("%s", m_primitive);
        }
    }

    /**
     * Creates a ParquetType from the given external original type string.
     *
     * @param string
     *            the string to parse
     * @return the resulting ParquetType
     */
    public static ParquetType fromExternalOriginalTypeString(final String string) {
        if (string.startsWith("LIST")) {
            final String subtype = string.substring(string.indexOf('[') + 1, string.lastIndexOf(']'));
            final ParquetType element = ParquetType.fromExternalOriginalTypeString(subtype);
            return new ParquetType(element);

        } else if (string.contains("(")) { // UTF8 (BINARY)
            final String org = string.substring(0, string.indexOf(' '));
            final String prim = string.substring(string.indexOf('(') + 1, string.lastIndexOf(')'));
            return new ParquetType(PrimitiveTypeName.valueOf(prim), OriginalType.valueOf(org));

        } else {
            return new ParquetType(PrimitiveTypeName.valueOf(string));
        }
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if ((obj.getClass() != this.getClass())) {
            return false;
        }

        final ParquetType other = (ParquetType) obj;
        return m_primitive == other.m_primitive //
            && Objects.equals(m_logical, other.m_logical) //
            && m_original == other.m_original //
            && Objects.equals(m_listElementType, other.m_listElementType);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_primitive == null) ? 0 : m_primitive.hashCode());
        result = prime * result + ((m_logical == null) ? 0 : m_logical.hashCode());
        result = prime * result + ((m_original == null) ? 0 : m_original.hashCode());
        result = prime * result + ((m_listElementType == null) ? 0 : m_listElementType.hashCode());
        return result;
    }

    /**
     * Constructs a Parquet Type object with the given Name
     *
     * @param name
     *            the name for the Type
     * @return the Type object
     */
    public Type constructParquetType(final String name) {
        if (m_listElementType != null) {
            var element = m_listElementType.constructParquetType("element");
            return Types.optionalList().element(element).named(name);
        } else if (m_logical instanceof UUIDLogicalTypeAnnotation) {
            return Types.optional(m_primitive).as(m_logical).length(UUIDLogicalTypeAnnotation.BYTES).named(name);
        } else if (m_logical != null) {
            return Types.optional(m_primitive).as(m_logical).named(name);
        } else if (m_original != null) {
            return Types.optional(m_primitive).as(m_original).named(name);
        } else {
            return Types.optional(m_primitive).named(name);
        }
    }

}
