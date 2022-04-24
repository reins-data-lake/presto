package com.facebook.presto.spi.plan;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SubTableScanNode extends TableScanNode{

    public SubTableScanNode(Optional<SourceLocation> sourceLocation, PlanNodeId id, TableHandle table, List<VariableReferenceExpression> outputVariables, Map<VariableReferenceExpression, ColumnHandle> assignments, TupleDomain<ColumnHandle> currentConstraint, TupleDomain<ColumnHandle> enforcedConstraint) {
        super(sourceLocation, id, table, outputVariables, assignments, currentConstraint, enforcedConstraint);
    }
}
