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
        TURN_RIGHT("turnRight", "右转"),
        /** 手刹：controlParams.on 0/1 */
        EPB("epb", "手刹"),
        /** 喇叭：controlParams.on 0/1 */
        HORN("horn", "喇叭"),
        /** 近光：controlParams.on 0/1 */
        LOW_BEAM("lowBeam", "近光灯"),
        /** 远光：controlParams.on 0/1 */
        HIGH_BEAM("highBeam", "远光灯"),
        /** 双闪：controlParams.on 0/1 */
        HAZARD_LIGHT("hazardLight", "双闪灯"),
        /** 辅助灯：controlParams.on 0/1 */
        AUX_LIGHT("auxLight", "辅助灯"),
        /** 装卸货灯：controlParams.green_light_cmd 0/1 */
        GREEN_LIGHT("greenLight", "装卸货灯"),
        /** 驾驶模式：controlParams.mode 0=M手动 1=A智驾 2=R远控(平行) — 与车端 parallelDrivingControl + e2e driving_mode 一致 */
        DRIVE_MODE("driveMode", "驾驶模式"),
        /** MRC 紧急停车：controlParams.mrc_status 0=正常 1=MRC0 2=MRC1 3=MRC2（M/A/R 任意模式可触发） */
        MRC("mrc", "MRC紧急停车");
        
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
     * Translate controlType + controlParams into the flat C++ field names and populate
     * this FunctionInvokeMessage's inputs list.
     *
     * Canonical wire format (functionId = "parallelDrivingControl"):
     *   drive_mode    : 0/1/2
     *   hbh_li_cmd    : 0=off, 1=on
     *   lbh_li_cmd    : 0=off, 1=on
     *   horn_cmd      : 0=off, 1=honk  (one-shot: cloud clears cache after each publish)
     *   hazard_li_cmd : 0=off, 1=on
     *   epb_cmd       : 0=释放, 1=拉起
     *   aux_li_cmd    : 0=off, 1=on
     *   green_light_cmd : 0=off, 1=on
     *
     * Call this method once before forwarding the message to the vehicle.
     */
    public void prepareInputs() {
        if (controlType == null) return;

        switch (controlType) {
            case DRIVE_MODE: {
                addInput("controlType", "drive_mode");
                Object mode = controlParams.get("mode");
                if (mode != null) addInput("drive_mode", mode);
                break;
            }
            case HIGH_BEAM: {
                addInput("controlType", "hbh_light");
                Object on = controlParams.get("on");
                if (on != null) addInput("hbh_li_cmd", toInt(on));  // 0=off, 1=on
                break;
            }
            case LOW_BEAM: {
                addInput("controlType", "lbh_light");
                Object on = controlParams.get("on");
                if (on != null) addInput("lbh_li_cmd", toInt(on));  // 0=off, 1=on
                break;
            }
            case HORN: {
                addInput("controlType", "horn");
                Object on = controlParams.get("on");
                // horn is one-shot: 1=honk, 0=off
                if (on != null) addInput("horn_cmd", toInt(on) == 1 ? 1 : 0);
                break;
            }
            case HAZARD_LIGHT: {
                addInput("controlType", "hazard_light");
                Object on = controlParams.get("on");
                if (on != null) addInput("hazard_li_cmd", toInt(on));  // 0=off, 1=on
                break;
            }
            case EPB: {
                addInput("controlType", "epb");
                Object on = controlParams.get("on");
                if (on != null) addInput("epb_cmd", toInt(on));  // 0=释放, 1=拉起
                break;
            }
            case AUX_LIGHT: {
                addInput("controlType", "aux_light");
                Object on = controlParams.get("on");
                if (on != null) addInput("aux_li_cmd", toInt(on));  // 0=off, 1=on
                break;
            }
            case GREEN_LIGHT: {
                addInput("controlType", "green_light");
                Object command = controlParams.get("green_light_cmd");
                if (command != null) addInput("green_light_cmd", toInt(command));  // 0=off, 1=on
                break;
            }
            case MRC: {
                addInput("controlType", "mrc");
                Object status = controlParams.get("mrc_status");
                if (status != null) addInput("mrc_status", toInt(status));
                Object selfRecoverStatus = controlParams.get("self_recover_status");
                if (selfRecoverStatus != null) {
                    addInput("self_recover_status", toInt(selfRecoverStatus));
                }
                break;
            }
            default:
                // Other control types (STEERING, BRAKE, etc.) keep existing controlParams as-is
                controlParams.forEach(this::addInput);
                break;
        }
    }

    /** Convert on/off integer (1=on, 0=off) to cmd encoding (1=on, 2=off, 0=no-op). */
    private static int onToCmd(Object on) {
        int v = toInt(on);
        return v == 1 ? 1 : 2;
    }

    private static int toInt(Object v) {
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(String.valueOf(v)); } catch (Exception e) { return 0; }
    }

    /**
     * @deprecated Use prepareInputs() instead. This override was incorrect — it re-added
     * all controlParams on every addInput() call, causing duplicate entries.
     */
    @Override
    @Deprecated
    public FunctionInvokeMessage addInput(String name, Object value) {
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
