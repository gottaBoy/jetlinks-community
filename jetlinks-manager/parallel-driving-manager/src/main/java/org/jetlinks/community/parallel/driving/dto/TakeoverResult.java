package org.jetlinks.community.parallel.driving.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import org.jetlinks.community.parallel.driving.enums.ParallelDrivingSessionState;

/**
 * 远程接管结果
 *
 * @author JetLinks
 */
@Data
@Schema(description = "远程接管结果")
public class TakeoverResult {
    
    @Schema(description = "驾驶舱设备ID")
    private String cockpitDeviceId;
    
    @Schema(description = "车辆设备ID")
    private String vehicleDeviceId;
    
    @Schema(description = "会话状态")
    private ParallelDrivingSessionState sessionState;
    
    @Schema(description = "是否成功")
    private Boolean success;
    
    @Schema(description = "消息")
    private String message;
}
