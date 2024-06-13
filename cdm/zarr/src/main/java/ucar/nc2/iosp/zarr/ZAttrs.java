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
      attrMap.keySet().stream().filter(key -> key.equals(NcZarrKeys.ARRAY_DIMENSIONS))
          .forEach(key -> {
            Object val = attrMap.get(key);
            if (val instanceof Collection<?>) {
              arrayDims.addAll((Collection<String>) val);
            }
          });

      return new ZAttrs(attrs, arrayDims);
    }

    private static Attribute createAttribute(String key, Object val, Map<String, Object> types) {
      Attribute.Builder attr = Attribute.builder(key);

      // Use ncZarr type attributes
      Object type = types.get(key);
      if (type != null) {
        try {
          DataType dataType = ZarrTypes.parseDataType((String) type);
          attr.setDataType(dataType);
          int size = val instanceof Collection<?> ? ((Collection<?>) val).size() : 1;
          Array array = Array.factory(dataType, new int[]{size});
          attr.setValues(array);
        } catch (ZarrFormatException e) {
          // try to use json types instead of explicit type
        }
      }

      // use json types
      if (val instanceof Collection<?>) {
        attr.setValues(Arrays.asList(((Collection) val).toArray()), false);
      } else if (val instanceof Number) {
        attr.setNumericValue((Number) val, false);
      } else if (val instanceof String ){
        attr.setStringValue((String) val);
      }

      return attr.build();
    }

    private static Map<String, Object> getTypes(Map<String, Object> attrMap) {
      Object ncZarrAttr = attrMap.get(NcZarrKeys.NC_ZARR_ATTR);
      Object types = ncZarrAttr instanceof Map<?, ?> ? ((Map<?, ?>) ncZarrAttr).get(NcZarrKeys.TYPES) : null;
      return types instanceof Map<?,?> ? (Map<String, Object>) types : new HashMap<>();
    }

    private static boolean includeAttributes(String key) {
      return !key.equals(NcZarrKeys.NC_ZARR_ATTR) && !key.equals((NcZarrKeys.ARRAY_DIMENSIONS));
    }
  }
}
