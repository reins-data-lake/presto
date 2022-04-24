package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import static java.util.Collections.unmodifiableMap;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.*;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;


public class TableScanToUnionAll
        implements Rule<TableScanNode> {
    private static final Pattern<TableScanNode> PATTERN = tableScan().matching(TableScanToUnionAll::notSub);
    private static boolean notSub(TableScanNode tableScanNode){
        return !SubTableScanNode.class.isInstance(tableScanNode);
    }

    @Override
    public Pattern<TableScanNode> getPattern() {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode node, Captures captures, Context context) {
        String[] nodeNames = new String[]{"example","example"};
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputs = new HashMap<>();
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        for(String nodeName:nodeNames) {
            ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>();
//        Map<VariableReferenceExpression, VariableReferenceExpression> oldToNew = new HashMap<>();

            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                ColumnHandle columnHandle = entry.getValue();
                VariableReferenceExpression newVariable = context.getVariableAllocator().newVariable(variable.getSourceLocation(), "modVar", variable.getType());
                assignments.put(newVariable, columnHandle);
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
                    node.getTable(),
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
