package org.apache.drill.exec.planner.sql.handlers;

import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.eigenbase.sql.SqlNode;

import java.io.IOException;

/**
 * Created by mbaid on 9/29/14.
 */
public class HiveSqlHandler extends AbstractSqlHandler {
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {
    return null;
  }
}
