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

package com.gigaspaces.query.sql.functions;

import com.gigaspaces.query.sql.functions.extended.LocalSession;

/**
 * Defines the arguments to be passed to an SqlFunction, generated for sqlFunction invocations.
 *
 * @author Tamir Schwarz
 * @since 11.0.0
 */
public interface SqlFunctionExecutionContext {
    /**
     * @return the number of arguments stored in the context
     */
    int getNumberOfArguments();

    /**
     * Provides mapping between stored arguments and the argument index to be passed to the
     * function
     *
     * @param index the index of the argument requested
     * @return the value of argument at 'index'
     */
    Object getArgument(int index);

    default LocalSession getSession() {
        return null;
    };

}
