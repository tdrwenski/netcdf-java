package ucar.nc2.iosp.zarr;

import static ucar.nc2.iosp.zarr.ZarrTypes.parseDataType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import ucar.ma2.DataType;
import ucar.nc2.filter.Filter;
import ucar.nc2.filter.Filters;
import ucar.nc2.filter.UnknownFilterException;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Java representation of .zarray metadata
 */
@JsonDeserialize(using = ZArray.ZArrayDeserializer.class)
public class ZArray {

  /**
   * Column or row order
   */
  public enum Order {
    C, F
  }

  private static final Set<String> VALID_SEPARATORS =
      Stream.of(".", "/").collect(Collectors.toCollection(HashSet::new));
  public static final String DEFAULT_SEPARATOR = ".";

  // .zarray fields
  private final int[] shape;
  private final int[] chunks;
  private final Object fillValue;
  private final DataType datatype;
  private final String dtype;
  private final Filter compressor;
  private final ByteOrder byteOrder;
  private final Order order;
  private final List<Filter> filters;
  private final String separator;

  public ZArray(int[] shape, int[] chunks, Object fill_value, String dtype, Filter compressor, String order,
      List<Filter> filters, String separator) throws ZarrFormatException {
    this.shape = shape;
    this.chunks = chunks;
    this.fillValue = fill_value;
    this.dtype = dtype;
    this.datatype = parseDataType(this.dtype);
    this.byteOrder = parseByteOrder(this.dtype);
    this.compressor = compressor;
    this.filters = filters;
    this.order = parseOrder(order);
    this.separator = validateSeparator(separator);
  }

  public int[] getShape() {
    return shape;
  }

  public int[] getChunks() {
    return this.chunks;
  }

  public Filter getCompressor() {
    return this.compressor;
  }

  public List<Filter> getFilters() {
    return this.filters;
  }

  public Object getFillValue() {
    return fillValue;
  }

  public Order getOrder() {
    return this.order;
  }

  public String getSeparator() {
    return this.separator;
  }

  public String getDtype() {
    return this.dtype;
  }

  public DataType getDataType() {
    return this.datatype;
  }

  public ByteOrder getByteOrder() {
    return this.byteOrder;
  }

  private static ByteOrder parseByteOrder(String dtype) throws ZarrFormatException {
    if (dtype.startsWith(">")) {
      return ByteOrder.BIG_ENDIAN;
    } else if (dtype.startsWith("<")) {
      return ByteOrder.LITTLE_ENDIAN;
    } else if (dtype.startsWith("|")) {
      return ByteOrder.nativeOrder();
    }
    throw new ZarrFormatException(ZarrKeys.DTYPE, dtype);
  }

  private static Order parseOrder(String order) throws ZarrFormatException {
    try {
      return Order.valueOf(order);
    } catch (IllegalArgumentException ex) {
      throw new ZarrFormatException(ZarrKeys.ORDER, order);
    }
  }

  private static String validateSeparator(String separator) throws ZarrFormatException {
    if (!VALID_SEPARATORS.contains(separator)) {
      throw new ZarrFormatException(ZarrKeys.DIMENSION_SEPARATOR, separator);
    }
    return separator;
  }

  static class ZArrayDeserializer extends StdDeserializer<ZArray> {

    protected ZArrayDeserializer() {
      super(ZArray.class);
    }

    @Override
    public ZArray deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      ObjectCodec codec = p.getCodec();
      TreeNode root = codec.readTree(p);
      int[] shape = StreamSupport.stream(((ArrayNode) root.path(ZarrKeys.SHAPE)).spliterator(), false)
          .mapToInt(JsonNode::asInt).toArray();
      int[] chunks = StreamSupport.stream(((ArrayNode) root.path(ZarrKeys.CHUNKS)).spliterator(), false)
          .mapToInt(JsonNode::asInt).toArray();
      String dtype = ((JsonNode) root.path(ZarrKeys.DTYPE)).asText();

      JsonNode fillValueNode = (JsonNode) root.path(ZarrKeys.FILL_VALUE);
      final Object fill;
      if (fillValueNode.isInt()) {
        fill = fillValueNode.asInt();
      } else if (fillValueNode.isLong()) {
        fill = fillValueNode.longValue();
      } else if (fillValueNode.isFloat()) {
        fill = fillValueNode.floatValue();
      } else if (fillValueNode.isNumber()) {
        fill = fillValueNode.asDouble();
      } else {
        fill = fillValueNode.asText("");
      }

      String order = ((JsonNode) root.path(ZarrKeys.ORDER)).asText();

      TreeNode dim_sep = root.path(ZarrKeys.DIMENSION_SEPARATOR);
      String delimiter = dim_sep.isMissingNode() ? DEFAULT_SEPARATOR : ((JsonNode) dim_sep).asText();

      // Filters and compressor
      try {
        Map<String, Object> compBean = codec.readValue(root.path(ZarrKeys.COMPRESSOR).traverse(codec), HashMap.class);

        Filter compressor = Filters.getFilter(compBean);

        List<Filter> filters = new ArrayList<>();

        Map<String, Object>[] filtersBean =
            codec.readValue(root.path(ZarrKeys.FILTERS).traverse(codec), HashMap[].class);

        if (filtersBean != null) {
          for (Map<String, Object> bean : filtersBean) {
            filters.add(Filters.getFilter(bean));
          }
        }
        return new ZArray(shape, chunks, fill, dtype, compressor, order, filters, delimiter);
      } catch (UnknownFilterException | ZarrFormatException ex) {
        throw new IOException(ex.getMessage(), ex.getCause());
      }
    }
  }
}
