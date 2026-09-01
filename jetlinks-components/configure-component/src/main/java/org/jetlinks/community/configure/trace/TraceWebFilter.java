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
package org.jetlinks.community.configure.trace;

import io.opentelemetry.api.trace.SpanKind;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.core.trace.ReactiveSpan;
import org.jetlinks.core.trace.ReactiveSpanBuilder;
import org.jetlinks.core.trace.TraceHolder;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class TraceWebFilter implements WebFilter, Ordered {
    @SuppressWarnings("all")
    @Override
    public Mono<Void> filter(ServerWebExchange exchange,
                             WebFilterChain chain) {
        //    /http/method/path
        String spanName = "/http/"+exchange.getRequest().getMethod().name()  + exchange.getRequest().getPath().value();

        ServerHttpRequest.Builder requestCopy = exchange
            .getRequest()
            .mutate();

        BiConsumer<ReactiveSpan, Boolean> onComplete = (span, ignored) -> {
            if (exchange.getResponse().getStatusCode() != null) {
                span.setAttribute("http.status_code", exchange.getResponse().getStatusCode().value());
                span.setAttribute(
                    "http.response.status_code",
                    exchange.getResponse().getStatusCode().value());
            }
        };
        Consumer<ReactiveSpanBuilder> spanBuilder = builder -> {
            builder
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("http.method", exchange.getRequest().getMethod().name())
                .setAttribute("http.request.method", exchange.getRequest().getMethod().name())
                .setAttribute("http.url", exchange.getRequest().getURI().toString())
                .setAttribute("url.full", exchange.getRequest().getURI().toString())
                .setAttribute("http.route", exchange.getRequest().getPath().value())
                .setAttribute("url.path", exchange.getRequest().getPath().value());

            InetSocketAddress localAddress = exchange.getRequest().getLocalAddress();
            if (localAddress != null) {
                if (localAddress.getHostString() != null) {
                    builder.setAttribute("server.address", localAddress.getHostString());
                }
                builder.setAttribute("server.port", localAddress.getPort());
            }
        };

        return TraceHolder
            //将追踪信息返回到响应头
            .writeContextTo(exchange.getResponse().getHeaders(), HttpHeaderTraceWriter.INSTANCE)
            //传递到下游请求头中
            .then(TraceHolder.writeContextTo(requestCopy, HttpServerHeaderTraceWriter.INSTANCE))
            //do filter
            .then(Mono.defer(() -> chain.filter(exchange.mutate().request(requestCopy.build()).build())))
            //创建跟踪信息
            .as(MonoTracer.<Void>create(
                TraceHolder.appName(),
                spanName,
                null,
                onComplete,
                spanBuilder))
            //从请求头中追加上级跟踪信息
            .contextWrite(ctx -> {
                return TraceHolder.readToContext(
                    ctx,
                    exchange.getRequest().getHeaders(),
                    HttpHeadersGetter.INSTANCE);
            });
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE + 100;
    }
}
