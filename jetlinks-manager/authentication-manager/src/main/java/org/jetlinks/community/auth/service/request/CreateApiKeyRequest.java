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
package org.jetlinks.community.auth.service.request;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

/**
 * 创建 API Key 请求
 *
 * @author JetLinks
 * @since 2.10.0
 */
@Getter
@Setter
public class CreateApiKeyRequest {

    @Schema(description = "用户ID（可选，不填则使用当前登录用户）")
    private String userId;

    @Schema(description = "应用名称")
    private String app;

    @Schema(description = "备注")
    private String remark;

    @Schema(description = "过期时间（时间戳，毫秒），null表示永不过期")
    private Long expiredAt;
}
