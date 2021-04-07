/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.Collections;

public class ExplainPlanResult extends QueryResult {
    public ExplainPlanResult(String explainPlanString) {
        super(Collections.singletonList(new ExplainPlanQueryColumn()));
        for (String row : explainPlanString.split("\n")) {
            add(new ExplainPlanTableRow(getQueryColumns().toArray(new QueryColumn[0]), new Object[]{row}));
        }
    }
}