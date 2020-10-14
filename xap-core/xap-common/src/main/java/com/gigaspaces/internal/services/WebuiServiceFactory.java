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
package com.gigaspaces.internal.services;

import com.gigaspaces.start.ClassLoaderType;
import com.gigaspaces.start.ClasspathBuilder;
import com.gigaspaces.start.SystemLocations;

public class WebuiServiceFactory extends ServiceFactory {
    @Override
    public String getServiceName() {
        return "WEBUI";
    }

    @Override
    protected String getServiceClassName() {
        return "org.openspaces.launcher.Launcher";
    }

    @Override
    protected void initializeClasspath(ClasspathBuilder classpath) {
        SystemLocations locations = SystemLocations.singleton();
        classpath
                // $GS_JARS
                .appendLibRequiredJars(ClassLoaderType.COMMON)
                .appendLibRequiredJars(ClassLoaderType.SERVICE)
                .appendJars(locations.libPlatformExt())
                .appendOptionalJars("spring").appendJars(locations.libOptionalSecurity())        // $SPRING_JARS
                .appendOptionalJars("jetty").appendOptionalJars("jetty/xap-jetty")
                .appendOptionalJars("interop")
                .appendOptionalJars("memoryxtend/off-heap")
                .appendOptionalJars("memoryxtend/rocksdb")
                .appendPlatformJars("commons")
                .appendPlatformJars("service-grid")
                .appendPlatformJars("zookeeper");
    }
}
