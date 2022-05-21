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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.reinsApi.GlobalIndexApi;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.*;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.*;
import java.util.stream.Stream;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.*;
import static java.util.Objects.requireNonNull;

public class RemoveFilters implements Rule<FilterNode> {

    private static final Capture<UnionNode> unionNodeCapture = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(union().capturedAs(unionNodeCapture)));
    @Override
    public Pattern<FilterNode> getPattern() {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context) {
        UnionNode unionNode = captures.get(unionNodeCapture);
        Map<String, Set<String>> predictCache = context.getSession().getPredictCache();
        String predictString = node.getPredicate().toString();
        Set<String> edgeNodesCache = predictCache.get(predictString);
        if(edgeNodesCache!=null && edgeNodesCache.size()==unionNode.getSources().size()){
            return Result.empty();
        }
        List<PlanNode> validNodes = new ArrayList<>();

        Map<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputs = new HashMap<>();
        for(VariableReferenceExpression v: unionNode.getOutputVariables()){
            outputToInputs.put(v, new ArrayList<>());
        }
        Map<VariableReferenceExpression, VariableReferenceExpression> reverseMap = new HashMap<>();
        for(Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry: unionNode.getVariableMapping().entrySet()){
            VariableReferenceExpression out = entry.getKey();
            for(VariableReferenceExpression in: entry.getValue()){
                reverseMap.put(in, out);
            }
        }
        try {
            if(edgeNodesCache==null){
                Set<String> edgeNodes = GlobalIndexApi.getNodes(node.getPredicate());
                predictCache.put(node.getPredicate().toString(), edgeNodes);
                edgeNodesCache = edgeNodes;
            }

            List<PlanNode> sources = unionNode.getSources();
            for(PlanNode childNode: sources){
                Stream<PlanNode> planNodeStream =  context.getLookup().resolveGroup(childNode);
                Set<String> finalEdgeNodesCache = edgeNodesCache;
                planNodeStream.forEach(n->{
                    if(n instanceof  TableScanNode){
                        TableScanNode tableScanNode = (TableScanNode) n;
                        if(finalEdgeNodesCache.contains(tableScanNode.getTable().getConnectorId().getCatalogName())){
                            validNodes.add(childNode);
                            for(VariableReferenceExpression in: childNode.getOutputVariables()){
                                VariableReferenceExpression out = reverseMap.get(in);
                                outputToInputs.get(out).add(in);
                            }
                        }
                    }
                });
            }
            if(validNodes.size()==0){
                return Result.empty();
            }
            List<PlanNode> newChildren = new ArrayList<>();
            newChildren.add(
                    new UnionNode(
                        unionNode.getSourceLocation(),
                        unionNode.getId(),
                        validNodes,
                        unionNode.getOutputVariables(),
                        outputToInputs
            ));

            return Result.ofPlanNode(node.replaceChildren(
                    newChildren
            ));
        }catch (Exception e){
            System.out.println(e.getMessage());
            return Result.empty();
        }



    }
}
