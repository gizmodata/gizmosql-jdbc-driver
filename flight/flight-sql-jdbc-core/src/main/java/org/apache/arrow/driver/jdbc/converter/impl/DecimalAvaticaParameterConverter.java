/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.driver.jdbc.converter.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;

/** AvaticaParameterConverter for Decimal Arrow types. */
public class DecimalAvaticaParameterConverter extends BaseAvaticaParameterConverter {

  public DecimalAvaticaParameterConverter(ArrowType.Decimal type) {}

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    if (!(vector instanceof DecimalVector)) {
      return false;
    }
    final DecimalVector dv = (DecimalVector) vector;
    final Object raw = typedValue == null ? null : typedValue.toLocal();
    if (raw == null) {
      dv.setNull(index);
      return true;
    }

    // Avatica often hands us numeric literals typed as Double/Long/Integer rather than
    // BigDecimal — e.g. when DBeaver's Data Editor accepts "3" from the user and routes
    // it as a DOUBLE with no column-type hint. A naked cast to BigDecimal would throw
    // ClassCastException, surfacing as the opaque error
    // "Binding value of type DOUBLE is not yet supported for expected Arrow type
    // Decimal(p, s, 128)". Coerce via toString() to avoid the well-known
    // `new BigDecimal(double)` precision pitfall.
    final BigDecimal value;
    if (raw instanceof BigDecimal) {
      value = (BigDecimal) raw;
    } else if (raw instanceof Number) {
      value = new BigDecimal(raw.toString());
    } else if (raw instanceof String) {
      value = new BigDecimal((String) raw);
    } else {
      return false;
    }

    // Arrow's DecimalVector#setSafe requires the BigDecimal's scale to match the vector's
    // declared scale exactly. Rescale to match — extending scale is loss-free; truncating
    // scale applies HALF_UP rounding so "1.2345" into DECIMAL(p,2) stores 1.23, matching
    // what every other JDBC driver does here.
    final int targetScale = dv.getScale();
    final BigDecimal scaled =
        value.scale() == targetScale ? value : value.setScale(targetScale, RoundingMode.HALF_UP);
    dv.setSafe(index, scaled);
    return true;
  }

  @Override
  public AvaticaParameter createParameter(Field field) {
    return createParameter(field, true);
  }
}
