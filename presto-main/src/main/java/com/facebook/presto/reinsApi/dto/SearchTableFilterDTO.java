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
package com.facebook.presto.reinsApi.dto;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SearchTableFilterDTO {
    List<Long> idList;
    long startTime;
    long endTime;


    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setIdList(List<Long> idList) {
        this.idList = idList;
    }
    @JsonProperty
    public List<Long> getIdList() {
        return idList;
    }
    @JsonProperty
    public long getEndTime() {
        return endTime;
    }
    @JsonProperty
    public long getStartTime() {
        return startTime;
    }
}