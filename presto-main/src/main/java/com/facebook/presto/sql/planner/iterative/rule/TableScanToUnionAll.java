/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import static java.util.Collections.unmodifiableMap;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.*;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.facebook.presto.metadata.Metadata;
import java.util.*;

import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;


public class TableScanToUnionAll
        implements Rule<TableScanNode> {
    Metadata metadata;
    public TableScanToUnionAll(Metadata metadata){
        this.metadata = requireNonNull(metadata, "metadata is null");
    }
    public class SubTableScanNode extends TableScanNode{

        public SubTableScanNode(Optional<SourceLocation> sourceLocation, PlanNodeId id, TableHandle table, List<VariableReferenceExpression> outputVariables, Map<VariableReferenceExpression, ColumnHandle> assignments, TupleDomain<ColumnHandle> currentConstraint, TupleDomain<ColumnHandle> enforcedConstraint) {
            super(sourceLocation, id, table, outputVariables, assignments, currentConstraint, enforcedConstraint);
        }
    }

    private static final Pattern<TableScanNode> PATTERN = tableScan().matching(TableScanToUnionAll::notSub);
    private static boolean notSub(TableScanNode tableScanNode){
        return !(tableScanNode instanceof SubTableScanNode);
    }

    @Override
    public Pattern<TableScanNode> getPattern() {
        return PATTERN;
    }
    private  String getColumnName(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
    }
    private TableHandle getTableHandleByDBName(Session session, TableHandle prevHandle, String dbName){
        SchemaTableName schemaTableName =  metadata.getTableMetadata(session, prevHandle).getMetadata().getTable();
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, new QualifiedObjectName(dbName, schemaTableName.getSchemaName(), schemaTableName.getTableName()));
        if(tableHandle.isPresent()){
            return tableHandle.get();
        }else {
            throw new RuntimeException("table not found");
        }
    }
    private  ColumnHandle getColumnHandelByTableNameAndColumnName(Session session, TableHandle prevHandle, String dbName, String colName){
        TableHandle tableHandle = getTableHandleByDBName(session, prevHandle, dbName);
        return metadata.getColumnHandles(session, tableHandle).get(colName);

    }


    @Override
    public Result apply(TableScanNode node, Captures captures, Context context) {
        //TODO: more nodes
        String[] connectorNames = new String[]{"mongo","mongo2","mongo3","mongo4","mongo","mongo5","mongo","mongo6","mongo","mongo7","mongo8","mongo9","delta"};
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputs = new HashMap<>();
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        for(String connectorName:connectorNames) {
            ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>();
            TableHandle tableHandle = getTableHandleByDBName(context.getSession(), node.getTable(), connectorName);
          for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                String colName = getColumnName(context.getSession(), node.getTable(), entry.getValue());
                ColumnHandle newColHandle = getColumnHandelByTableNameAndColumnName(context.getSession(), node.getTable(), connectorName, colName);
//                ExampleColumnHandle columnHandle = (ExampleColumnHandle) entry.getValue();
                VariableReferenceExpression newVariable = context.getVariableAllocator().newVariable(variable.getSourceLocation(), "modVar", variable.getType());
                assignments.put(newVariable, newColHandle);
                outputs.add(newVariable);
                if (outputToInputs.containsKey(variable)) {
                    outputToInputs.get(variable).add(newVariable);
                } else {
                    List<VariableReferenceExpression> inputs = new ArrayList<>();
                    inputs.add(newVariable);
                    outputToInputs.put(variable, inputs);
                }
            }
            SubTableScanNode newNode = new SubTableScanNode(node.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    tableHandle,
                    outputs.build(),
                    assignments,
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint()
            );
            sources.add(newNode);
        }


        return Result.ofPlanNode(
                new UnionNode(node.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        sources.build(),
                        node.getOutputVariables(),
                        unmodifiableMap(outputToInputs)
                        )
        );
    }
}
