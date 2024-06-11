package ucar.nc2.iosp.zarr;

import java.util.HashMap;
import java.util.Map;
import ucar.ma2.DataType;

public class ZarrTypes {
  // maps zarr datatypes to CDM datatypes
  private static Map<String, DataType> dTypeMap;

  static {
    dTypeMap = new HashMap<>();
    dTypeMap.put("b1", DataType.BOOLEAN);
    dTypeMap.put("i1", DataType.BYTE);
    dTypeMap.put("S1", DataType.CHAR);
    dTypeMap.put("U1", DataType.CHAR);
    dTypeMap.put("O1", DataType.OBJECT);
    dTypeMap.put("u1", DataType.UBYTE);
    dTypeMap.put("i2", DataType.SHORT);
    dTypeMap.put("u2", DataType.USHORT);
    dTypeMap.put("i4", DataType.INT);
    dTypeMap.put("f4", DataType.FLOAT);
    dTypeMap.put("S4", DataType.STRING);
    dTypeMap.put("U4", DataType.STRING);
    dTypeMap.put("u4", DataType.UINT);
    dTypeMap.put("i8", DataType.LONG);
    dTypeMap.put("f8", DataType.DOUBLE);
    dTypeMap.put("u8", DataType.ULONG);
  }

  static DataType parseDataType(String dtype) throws ZarrFormatException {
    dtype = dtype.replace(">", "");
    dtype = dtype.replace("<", "");
    dtype = dtype.replace("|", "");
    DataType dataType = dTypeMap.get(dtype);
    if (dataType == null) {
      throw new ZarrFormatException(ZarrKeys.DTYPE, dtype);
    }
    return dataType;
  }
}
