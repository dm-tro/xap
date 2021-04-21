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
package com.gigaspaces.jdbc;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

public class MyPojo {
    private String id;
    private String name;
    private Integer age;
    private String country;
    private Date birthDate;
    private Time birthTime;
    private Timestamp timestamp;
    private Long birthLong;

    public MyPojo() {
    }

    public MyPojo(String name, Integer age, String country, Date birthDate, Time birthTime, Timestamp timestamp) {
        this.name = name;
        this.age = age;
        this.country = country;
        this.birthDate = birthDate;
        this.birthTime = birthTime;
        this.timestamp = timestamp;
        this.birthLong = birthDate.getTime();
    }

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public MyPojo setId(String id) {
        this.id = id;
        return this;
    }

    @SpaceIndex
    public String getName() {
        return name;
    }

    public MyPojo setName(String name) {
        this.name = name;
        return this;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public Integer getAge() {
        return age;
    }

    public MyPojo setAge(Integer age) {
        this.age = age;
        return this;
    }

    public String getCountry() {
        return country;
    }

    public MyPojo setCountry(String country) {
        this.country = country;
        return this;
    }

    public Date getBirthDate() {
        return birthDate;
    }

    public MyPojo setBirthDate(Date birthDate) {
        this.birthDate = birthDate;
        return this;
    }

    public Time getBirthTime() {
        return birthTime;
    }

    public MyPojo setBirthTime(Time birthTime) {
        this.birthTime = birthTime;
        return this;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public MyPojo setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Long getBirthLong() {
        return birthLong;
    }

    public MyPojo setBirthLong(Long birthLong) {
        this.birthLong = birthLong;
        return this;
    }

    @Override
    public String toString() {
        return "MyPojo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", country='" + country + '\'' +
                ", birthDate=" + birthDate +
                ", birthTime=" + birthTime +
                ", timestamp=" + timestamp +
                '}';
    }
}
