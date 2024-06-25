package ucar.nc2.iosp.zarr;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;

/**
 * Java representation of .zattrs metadata
 */
@JsonDeserialize(using = ZAttrs.ZAttrsDeserializer.class)
public class ZAttrs {
  private final List<Attribute> attributes;
  private final List<String> arrayDimensions;

  public ZAttrs(List<Attribute> attributes, List<String> arrayDimensions) {
    this.attributes = attributes;
    this.arrayDimensions = arrayDimensions;
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  public List<String> getArrayDimensions() {
    return arrayDimensions;
  }

  static class ZAttrsDeserializer extends StdDeserializer<ZAttrs> {
    protected ZAttrsDeserializer() {
      super(ZAttrs.class);
    }

    @Override
    public ZAttrs deserialize(JsonParser parser, DeserializationContext context) throws IOException {
      ObjectCodec codec = parser.getCodec();
      Map<String, Object> attrMap = codec.readValue(parser, HashMap.class);
      Map<String, Object> types = getTypes(attrMap);

      List<Attribute> attrs = new ArrayList<>();
      attrMap.keySet().stream().filter(ZAttrsDeserializer::includeAttributes)
          .forEach(key -> attrs.add(createAttribute(key, attrMap.get(key), types)));

      List<String> arrayDims = new ArrayList<>();
      attrMap.keySet().stream().filter(key -> key.equals(NcZarrKeys.ARRAY_DIMENSIONS)).forEach(key -> {
        Object val = attrMap.get(key);
        if (val instanceof Collection<?>) {
          arrayDims.addAll((Collection<String>) val);
        }
      });

      return new ZAttrs(attrs, arrayDims);
    }

    private static Attribute createAttribute(String key, Object val, Map<String, Object> types) {
      if (types.get(key) == null) {
        return buildWithJsonType(key, val);
      }

      try {
        DataType dataType = ZarrTypes.parseDataType((String) types.get(key));
        return buildWithType(key, val, dataType);
      } catch (ZarrFormatException e) {
        return buildWithJsonType(key, val);
      }
    }

    private static Attribute buildWithType(String key, Object value, DataType dataType) {
      Attribute.Builder attribute = Attribute.builder(key);
      attribute.setDataType(dataType);

      if (value instanceof Collection<?>) {
        Object[] values = ((Collection<?>) value).toArray();
        Array array = Array.factory(dataType, new int[] {values.length});

        for (int i = 0; i < values.length; i++) {
          array.setObject(i, values[i]);
        }
        attribute.setValues(array);
      } else {
        Array array = Array.factory(dataType, new int[] {1});
        array.setObject(0, value);
        attribute.setValues(array);
      }

      return attribute.build();
    }

    private static Attribute buildWithJsonType(String key, Object value) {
      Attribute.Builder attribute = Attribute.builder(key);

      if (value instanceof Collection<?>) {
        attribute.setValues(Arrays.asList(((Collection) value).toArray()), false);
      } else if (value instanceof Number) {
        attribute.setNumericValue((Number) value, false);
      } else if (value instanceof String) {
        attribute.setStringValue((String) value);
      }

      return attribute.build();
    }

    private static Map<String, Object> getTypes(Map<String, Object> attrMap) {
      Object ncZarrAttr = attrMap.get(NcZarrKeys.NC_ZARR_ATTR);
      Object types = ncZarrAttr instanceof Map<?, ?> ? ((Map<?, ?>) ncZarrAttr).get(NcZarrKeys.TYPES) : null;
      return types instanceof Map<?, ?> ? (Map<String, Object>) types : new HashMap<>();
    }

    private static boolean includeAttributes(String key) {
      return !key.equals(NcZarrKeys.NC_ZARR_ATTR) && !key.equals((NcZarrKeys.ARRAY_DIMENSIONS));
    }
  }
}
