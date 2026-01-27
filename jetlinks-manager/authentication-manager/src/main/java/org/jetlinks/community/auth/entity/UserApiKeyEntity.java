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
package org.jetlinks.community.auth.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import org.hswebframework.ezorm.rdb.mapping.annotation.*;
import org.hswebframework.web.api.crud.entity.EntityFactoryHolder;
import org.hswebframework.web.api.crud.entity.GenericEntity;
import org.hswebframework.web.api.crud.entity.RecordCreationEntity;
import org.hswebframework.web.api.crud.entity.RecordModifierEntity;
import org.hswebframework.web.crud.annotation.EnableEntityEvent;
import org.hswebframework.web.crud.generator.Generators;
import org.hswebframework.web.validator.CreateGroup;

import javax.persistence.Column;
import javax.persistence.Table;
import jakarta.validation.constraints.NotBlank;

/**
 * 用户 API Key 实体
 *
 * @author JetLinks
 * @since 2.10.0
 */
@Getter
@Setter
@Table(name = "user_api_key")
@Comment("用户API Key表")
@EnableEntityEvent
public class UserApiKeyEntity extends GenericEntity<Long> implements RecordCreationEntity, RecordModifierEntity {

    @Schema(description = "用户ID")
    @Column(nullable = false, length = 64)
    @NotBlank(groups = CreateGroup.class, message = "用户ID不能为空")
    private String userId;

    @Schema(description = "应用名称")
    @Column(length = 255)
    private String app;

    @Schema(description = "API Key")
    @Column(nullable = false, length = 500)
    @NotBlank(groups = CreateGroup.class, message = "API Key不能为空")
    private String apiKey;

    @Schema(description = "过期时间（时间戳，毫秒），null表示永不过期")
    @Column
    private Long expiredAt;

    @Schema(description = "备注")
    @Column(length = 255)
    private String remark;

    @Schema(description = "是否启用，1为启用，0为禁用")
    @Column(nullable = false)
    @DefaultValue("1")
    private Byte enable;

    @Schema(description = "创建时间")
    @Column(nullable = false, updatable = false)
    @DefaultValue(generator = Generators.CURRENT_TIME)
    private Long createTime;

    @Schema(description = "修改时间")
    @Column
    @DefaultValue(generator = Generators.CURRENT_TIME)
    private Long modifyTime;

    @Schema(description = "创建人ID")
    @Column(length = 64, updatable = false)
    private String creatorId;

    @Schema(description = "创建人名称")
    @Column
    @Upsert(insertOnly = true)
    private String creatorName;

    @Schema(description = "修改人ID")
    @Column(length = 64)
    private String modifierId;

    @Schema(description = "修改人名称")
    @Column
    private String modifierName;

    /**
     * 检查 API Key 是否已过期
     */
    public boolean isExpired() {
        if (expiredAt == null) {
            return false; // 永不过期
        }
        return System.currentTimeMillis() > expiredAt;
    }

    /**
     * 检查 API Key 是否有效（启用且未过期）
     */
    public boolean isValid() {
        return enable != null && enable == 1 && !isExpired();
    }

    public static UserApiKeyEntity of() {
        return EntityFactoryHolder.newInstance(UserApiKeyEntity.class, UserApiKeyEntity::new);
    }
}
