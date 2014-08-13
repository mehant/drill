package org.apache.drill.common.util;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos;

public class CoreDecimalUtility {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoreDecimalUtility.class);

  public static long getDecimal18FromBigDecimal(BigDecimal input, int scale, int precision) {
    // Truncate or pad to set the input to the correct scale
    input = input.setScale(scale, BigDecimal.ROUND_HALF_UP);

    return (input.unscaledValue().longValue());
  }

  public static int getMaxPrecision(TypeProtos.MinorType decimalType) {
    if (decimalType == TypeProtos.MinorType.DECIMAL9) {
      return 9;
    } else if (decimalType == TypeProtos.MinorType.DECIMAL18) {
      return 18;
    } else if (decimalType == TypeProtos.MinorType.DECIMAL28SPARSE) {
      return 28;
    } else if (decimalType == TypeProtos.MinorType.DECIMAL38SPARSE) {
      return 38;
    }
    return 0;
  }

  /*
   * Function returns the Minor decimal type given the precision
   */
  public static TypeProtos.MinorType getDecimalDataType(int precision) {
    if (precision <= 9) {
      return TypeProtos.MinorType.DECIMAL9;
    } else if (precision <= 18) {
      return TypeProtos.MinorType.DECIMAL18;
    } else if (precision <= 28) {
      return TypeProtos.MinorType.DECIMAL28SPARSE;
    } else {
      return TypeProtos.MinorType.DECIMAL38SPARSE;
    }
  }

  /*
   * Given a precision it provides the max precision of that decimal data type;
   * For eg: given the precision 12, we would use DECIMAL18 to store the data
   * which has a max precision range of 18 digits
   */
  public static int getPrecisionRange(int precision) {
    return getMaxPrecision(getDecimalDataType(precision));
  }
  public static int getDecimal9FromBigDecimal(BigDecimal input, int scale, int precision) {
    // Truncate/ or pad to set the input to the correct scale
    input = input.setScale(scale, BigDecimal.ROUND_HALF_UP);

    return (input.unscaledValue().intValue());
  }
}
