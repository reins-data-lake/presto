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

import com.facebook.presto.reinsApi.dto.SearchTableFilterDTO;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

public class GlobalIndexApi {

    public static HashSet<String> getNodes(RowExpression predict) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        Gson gson = new Gson();
        HashSet<String> s = new HashSet<>();
        s.add("mongo");
        return s;
//        HttpPost request = new HttpPost("http://localhost:18880/searchNodes");
//        SearchTableFilterDTO searchTableFilterDTO = new SearchTableFilterDTO();
//
////        searchTableFilterDTO.setTopic();
//        //TODO:predict to filter
//        StringEntity postingString = new StringEntity(gson.toJson(searchTableFilterDTO));
//        request.setEntity(postingString);
//        request.setHeader("Content-type", "application/json");
//        HttpResponse response = client.execute(request);
//        String responseBody =EntityUtils.toString(response.getEntity());
//        return gson.fromJson(responseBody, new TypeToken<HashSet<String>>() {}.getType());
    }
}
