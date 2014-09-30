package org.apache.drill.exec.planner.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.apache.drill.exec.util.Pointer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.parser.SqlParseException;

import java.io.IOException;

/**
 * Created by mbaid on 9/29/14.
 */
public class HiveSqlWorker extends DrillSqlWorker {
  public HiveSqlWorker(QueryContext context) throws Exception {
    super(context);
  }

  @Override
  public PhysicalPlan getPlan(String sql, Pointer<String> textPlan) throws SqlParseException, ValidationException, RelConversionException, IOException {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set("hive.metastore.uris", "thrift://localhost:9083");

    Driver d = new Driver(hiveConf);

    try {
      RelNode rel= d.getOptiqLogicalPlan("Select * from kv");
      DefaultSqlHandler handler = new DefaultSqlHandler(getConfig());
      return handler.getPlanFromOptiqLogical(rel);
    } catch (Exception e) {
      new RelConversionException(e);
    }
    return null;
  }
}
