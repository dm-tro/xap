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

package com.gigaspaces.internal.utils.parsers;

import com.j_spaces.jdbc.QueryProcessor;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class SqlTimestampParser extends AbstractDateTimeParser {
    public SqlTimestampParser() {
        super("java.sql.Timestamp", QueryProcessor.getDefaultConfig().getTimestampFormat());
    }

    @Override
    public Object parse(String s) throws SQLException {
        // if the string to parse is shorter than the pattern it will fail, we will try parsing using the default
        // LocalDateTimeParser instead (ISO_LOCAL_DATE_TIME)
        if (s.length() < _pattern.length()){
            try{
                return Timestamp.valueOf(LocalDateTime.parse(s));
            }
            catch (Exception e){}
        }
        return new java.sql.Timestamp(parseDateTime(s.replaceAll("T"," ")).getTime());
    }
}
