///*
// * Copyright 2025 JetLinks https://www.jetlinks.cn
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.jetlinks.community.datasource.rdb;
//
//import org.hswebframework.ezorm.rdb.executor.SqlRequest;
//import org.hswebframework.ezorm.rdb.executor.reactive.ReactiveSqlExecutor;
//import org.hswebframework.ezorm.rdb.executor.wrapper.ResultWrapper;
//import org.reactivestreams.Publisher;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Primary;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//
///**
// * R2DBC执行器配置
// * 用于解决PostgreSQL R2DBC Long到Integer的类型转换问题
// */
//@Configuration
//public class R2DBCExecutorConfiguration {
//
//    /**
//     * 创建修复了Long到Integer转换问题的ReactiveSqlExecutor
//     * 这个Bean会替换hswebframework框架默认的ReactiveSqlExecutor
//     */
//    @Bean
//    @Primary
//    public ReactiveSqlExecutor fixedReactiveSqlExecutor(
//            @Qualifier("r2dbcReactiveSqlExecutor") ReactiveSqlExecutor originalExecutor) {
//        return new ReactiveSqlExecutor() {
//            @Override
//            public Mono<Integer> update(Publisher<SqlRequest> request) {
//                return originalExecutor.update(request)
//                    .cast(Number.class)
//                    .map(result -> {
//                        // Handle Long to Integer conversion for PostgreSQL R2DBC
//                        if (result instanceof Long longValue) {
//                            // 安全检查：确保Long值在Integer范围内
//                            if (longValue > Integer.MAX_VALUE) {
//                                return Integer.MAX_VALUE;
//                            }
//                            if (longValue < Integer.MIN_VALUE) {
//                                return Integer.MIN_VALUE;
//                            }
//                            return longValue.intValue();
//                        }
//                        return result.intValue();
//                    });
//            }
//
//            @Override
//            public Mono<Void> execute(Publisher<SqlRequest> request) {
//                return originalExecutor.execute(request);
//            }
//
//            @Override
//            public <E> Flux<E> select(Publisher<SqlRequest> request, ResultWrapper<E, ?> wrapper) {
//                return originalExecutor.select(request, wrapper);
//            }
//        };
//    }
//}