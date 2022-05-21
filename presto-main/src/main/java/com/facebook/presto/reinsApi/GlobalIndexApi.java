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
package com.facebook.presto.reinsApi;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.reinsApi.dto.ResDTO;
import com.facebook.presto.reinsApi.dto.SearchTableFilterDTO;
import com.facebook.presto.spi.relation.*;
import com.facebook.airlift.json.JsonCodec;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GlobalIndexApi {
    static HttpClient client;
    static URI uri;
    static {
        client = new JettyHttpClient();
        try {
            uri = new URI("http://localhost:18880/searchNodes");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void iterPredict(RowExpression predict, SearchTableFilterDTO searchTableFilterDTO){
        if(predict instanceof CallExpression){
            CallExpression leaf = (CallExpression) predict;
            if(leaf.getDisplayName().equals("BETWEEN")){
                if(leaf.getArguments().get(0) instanceof VariableReferenceExpression){
                    if(((VariableReferenceExpression) leaf.getArguments().get(0)).getName().equals("timestamp")){
                        searchTableFilterDTO.setStartTime((Long) ((ConstantExpression) leaf.getArguments().get(1)).getValue());
                        searchTableFilterDTO.setStartTime((Long) ((ConstantExpression) leaf.getArguments().get(2)).getValue());
                    }
                }
            }
        }else if (predict instanceof SpecialFormExpression) {
            SpecialFormExpression node = (SpecialFormExpression) predict;
            if(node.getForm().name().equals("IN")){
                List<RowExpression> args = node.getArguments();
                if(((VariableReferenceExpression) args.get(0)).getName().equals("taxiId")){
                    List<Long> ids = new ArrayList<>();
                    for(int i = 1; i <args.size();i++){
                        ids.add((Long) ((ConstantExpression) args.get(i)).getValue());
                    }
                    searchTableFilterDTO.setIdList(ids);
                }
            }else{
                for(RowExpression args : node.getArguments()){
                    iterPredict(args, searchTableFilterDTO);
                }
            }
        }
    }

    public static Set<String> getNodes(RowExpression predict) throws Exception {
        SearchTableFilterDTO searchTableFilterDTO = new SearchTableFilterDTO();
        iterPredict(predict, searchTableFilterDTO);

        JsonCodec<SearchTableFilterDTO> codec = JsonCodec.jsonCodec(SearchTableFilterDTO.class);
        Request request = preparePost().setUri(uri).setBodyGenerator(createStaticBodyGenerator(codec.toJson(searchTableFilterDTO), UTF_8)).build();
        JsonCodec<ResDTO> resDec = JsonCodec.jsonCodec(ResDTO.class);
        ResDTO queryResults = client.execute(request,createJsonResponseHandler(resDec));

        return queryResults.getNodes();
    }
}
