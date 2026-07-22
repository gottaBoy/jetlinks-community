/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.community.device.timeseries;

import org.jetlinks.community.ConfigMetadataConstants;
import org.jetlinks.community.timeseries.TimeSeriesMetadata;
import org.jetlinks.community.timeseries.TimeSeriesMetric;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.SimplePropertyMetadata;
import org.jetlinks.core.metadata.types.*;

import java.util.ArrayList;
import java.util.List;

class DevicePropertiesTimeSeriesMetadata implements TimeSeriesMetadata {

    /** 设备属性 JSON 等原始串可能超过默认 varchar(255)，仅放宽 value / formatValue */
    private static final long PROPERTY_VALUE_VARCHAR_LENGTH = 2048L;

    private final static List<PropertyMetadata> metadata = new ArrayList<>();

    private final TimeSeriesMetric metric;

    public DevicePropertiesTimeSeriesMetadata(String productId) {
        this.metric = DeviceTimeSeriesMetric.devicePropertyMetric(productId);
    }

    static {
        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("id");
            property.setValueType(StringType.GLOBAL);
            property.setName("id");
            metadata.add(property);
        }
        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("property");
            property.setValueType(new StringType());
            property.setName("属性标识");
            metadata.add(property);
        }
        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("propertyName");
            property.setValueType(new StringType());
            property.setName("属性名称");
            metadata.add(property);
        }
        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("value");
            property.setValueType(new StringType().expand(
                ConfigMetadataConstants.maxLength, PROPERTY_VALUE_VARCHAR_LENGTH));
            property.setName("原始值");
            metadata.add(property);
        }

        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("geoValue");
            property.setValueType(new GeoType());
            property.setName("地理位置");
            metadata.add(property);
        }

        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("objectValue");
            property.setValueType(new ObjectType());
            property.setName("对象值");
            metadata.add(property);
        }
        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("timeValue");
            property.setValueType(new DateTimeType());
            property.setName("时间值");
            metadata.add(property);
        }

        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("formatValue");
            property.setValueType(new StringType().expand(
                ConfigMetadataConstants.maxLength, PROPERTY_VALUE_VARCHAR_LENGTH));
            property.setName("格式化的值");
            metadata.add(property);
        }

        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("numberValue");
            property.setValueType(new DoubleType());
            property.setName("数字值");
            metadata.add(property);
        }

        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("deviceId");
            property.setValueType(new StringType());
            property.setName("设备ID");
            metadata.add(property);
        }
//
//        {
//            SimplePropertyMetadata property = new SimplePropertyMetadata();
//            property.setId("productId");
//            property.setValueType(new StringType());
//            property.setName("产品ID");
//            metadata.add(property);
//        }

        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("type");
            property.setValueType(new StringType());
            property.setName("类型");
            metadata.add(property);
        }

        {
            SimplePropertyMetadata property = new SimplePropertyMetadata();
            property.setId("createTime");
            property.setValueType(DateTimeType.GLOBAL);
            property.setName("创建时间");
            metadata.add(property);
        }
    }

    @Override
    public TimeSeriesMetric getMetric() {
        return metric;
    }

    @Override
    public List<PropertyMetadata> getProperties() {
        return new ArrayList<>(metadata);
    }
}
