/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlDropTable;
import org.apache.drill.exec.planner.sql.parser.SqlShowFiles;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DropTableHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropTableHandler.class);

  public DropTableHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {

    String tableName = ((SqlDropTable) sqlNode).getName();

    List<String> fullPath = ((SqlDropTable) sqlNode).getFullPath();
    SqlIdentifier table = ((SqlDropTable) sqlNode).getTableName();

    SchemaPlus defaultSchema = context.getNewDefaultSchema();
    SchemaPlus drillSchema = defaultSchema;

    if (table != null) {
      drillSchema = SchemaUtilites.findSchema(defaultSchema, table.names.subList(0, table.names.size() - 1));
    }

    if (drillSchema == null) {
      throw UserException.validationError()
          .message("Invalid table_name [%s]", table.toString())
          .build(logger);
    }

    WorkspaceSchemaFactory.WorkspaceSchema wsSchema;
    try {
      wsSchema = (WorkspaceSchemaFactory.WorkspaceSchema) drillSchema.unwrap(AbstractSchema.class).getDefaultSchema();
    } catch (ClassCastException e) {
      throw UserException.validationError()
          .message("SHOW FILES is supported in workspace type schema only. Schema [%s] is not a workspace schema.",
              SchemaUtilites.getSchemaPath(drillSchema))
          .build(logger);
    }
    DrillFileSystem fs = wsSchema.getFS();
    String defaultLocation = wsSchema.getDefaultLocation();

    fs.delete(new Path(defaultLocation, table.names.get(table.names.size() - 1)), true);

    return DirectPlan.createDirectPlan(context, true,
        String.format("Table [%s] dropped successfully ", tableName));
  }
}
