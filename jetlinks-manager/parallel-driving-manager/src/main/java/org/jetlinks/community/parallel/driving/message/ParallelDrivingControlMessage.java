package org.jetlinks.community.parallel.driving.message;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.message.function.FunctionInvokeMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * 平行驾驶控制指令消息
 * 基于 JetLinks FunctionInvokeMessage 扩展
 * 
 * @author JetLinks
 */
@Getter
@Setter
public class ParallelDrivingControlMessage extends FunctionInvokeMessage {
    
    /**
     * 控制指令类型
     */
    public enum ControlType {
        STEERING("steering", "转向控制"),
        ACCELERATOR("accelerator", "加速控制"),
        BRAKE("brake", "制动控制"),
        GEAR("gear", "档位控制"),
        EMERGENCY_STOP("emergencyStop", "紧急停车"),
        SET_SPEED("setSpeed", "设置速度"),
        TURN_LEFT("turnLeft", "左转"),
        TURN_RIGHT("turnRight", "右转");
        
        private final String value;
        private final String text;
        
        ControlType(String value, String text) {
            this.value = value;
            this.text = text;
        }
        
        public String getValue() {
            return value;
        }
        
        public String getText() {
            return text;
        }
    }
    
    /**
     * 控制指令类型
     */
    private ControlType controlType;
    
    /**
     * 控制参数
     * 例如：
     * - STEERING: { "angle": 15.5 }  // 转向角度（度）
     * - ACCELERATOR: { "value": 0.8 }  // 加速值（0-1）
     * - BRAKE: { "value": 0.5 }  // 制动力（0-1）
     * - GEAR: { "gear": 3 }  // 档位（1-5）
     * - SET_SPEED: { "speed": 60.0 }  // 目标速度（km/h）
     */
    private Map<String, Object> controlParams = new HashMap<>();
    
    public ParallelDrivingControlMessage() {
        setFunctionId("parallelDrivingControl");
    }
    
    /**
     * 创建转向控制指令
     */
    public static ParallelDrivingControlMessage steering(double angle) {
        ParallelDrivingControlMessage message = new ParallelDrivingControlMessage();
        message.setControlType(ControlType.STEERING);
        message.getControlParams().put("angle", angle);
        return message;
    }
    
    /**
     * 创建加速控制指令
     */
    public static ParallelDrivingControlMessage accelerator(double value) {
        ParallelDrivingControlMessage message = new ParallelDrivingControlMessage();
        message.setControlType(ControlType.ACCELERATOR);
        message.getControlParams().put("value", value);
        return message;
    }
    
    /**
     * 创建制动控制指令
     */
    public static ParallelDrivingControlMessage brake(double value) {
        ParallelDrivingControlMessage message = new ParallelDrivingControlMessage();
        message.setControlType(ControlType.BRAKE);
        message.getControlParams().put("value", value);
        return message;
    }
    
    /**
     * 创建档位控制指令
     */
    public static ParallelDrivingControlMessage gear(int gear) {
        ParallelDrivingControlMessage message = new ParallelDrivingControlMessage();
        message.setControlType(ControlType.GEAR);
        message.getControlParams().put("gear", gear);
        return message;
    }
    
    /**
     * 创建紧急停车指令
     */
    public static ParallelDrivingControlMessage emergencyStop() {
        ParallelDrivingControlMessage message = new ParallelDrivingControlMessage();
        message.setControlType(ControlType.EMERGENCY_STOP);
        return message;
    }
    
    /**
     * 创建设置速度指令
     */
    public static ParallelDrivingControlMessage setSpeed(double speed) {
        ParallelDrivingControlMessage message = new ParallelDrivingControlMessage();
        message.setControlType(ControlType.SET_SPEED);
        message.getControlParams().put("speed", speed);
        return message;
    }
    
    /**
     * 转换为 FunctionInvokeMessage 的 inputs
     * 用于兼容 JetLinks 标准消息格式
     */
    @Override
    public FunctionInvokeMessage addInput(String name, Object value) {
        // 将控制参数添加到 inputs
        if (controlParams != null && !controlParams.isEmpty()) {
            controlParams.forEach((key, val) -> {
                super.addInput(key, val);
            });
        }
        // 添加控制类型
        super.addInput("controlType", controlType != null ? controlType.getValue() : null);
        return super.addInput(name, value);
    }
    
    /**
     * 设置会话信息（自动填充）
     */
    public void setSessionInfo(String sessionId, String roomId, String operatorId) {
        if (sessionId != null) {
            addHeader("sessionId", sessionId);
        }
        if (roomId != null) {
            addHeader("roomId", roomId);
        }
        if (operatorId != null) {
            addHeader("operatorId", operatorId);
        }
        addHeader("sourceType", "cockpit");
        addHeader("messageType", "parallel-driving-control");
    }
}
