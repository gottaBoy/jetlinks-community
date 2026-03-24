package org.jetlinks.community.parallel.driving.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import org.jetlinks.community.device.enums.DeviceState;
import org.jetlinks.community.parallel.driving.enums.ParallelDrivingSessionState;

/**
 * 驾驶舱信息 DTO
 * 包含驾驶舱基本信息和绑定状态
 *
 * @author JetLinks
 */
@Data
@Schema(description = "驾驶舱信息")
public class CockpitInfo {
    
    @Schema(description = "设备ID")
    private String deviceId;
    
    @Schema(description = "设备名称")
    private String deviceName;
    
    @Schema(description = "产品ID")
    private String productId;
    
    @Schema(description = "在线状态")
    private DeviceState state;  // online/offline/notActive
    
    @Schema(description = "绑定的车辆ID（如果有）")
    private String boundVehicleId;
    
    @Schema(description = "会话状态")
    private ParallelDrivingSessionState sessionState;  // binding/active/releasing/released
    
    @Schema(description = "绑定时间")
    private Long bindTime;
    
    @Schema(description = "最后活动时间")
    private Long lastActiveTime;
}
