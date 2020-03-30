package com.ymhx.dataplatform.kafka.untils;

public enum  ADASEnum {

    FCW(" 前碰撞",1),
    UFCW(" 低速碰撞",2),
    LDW(" 车道左偏离",3),
    PCW(" 行人碰撞",4),
    HMW(" 车距检测",5),
    TSR(" 限速提示",6),
    LDWR(" 车道右偏离",7),
    ODA(" 障碍物检测警报",8),
    ABW(" 自动紧急刹车",9),
    AEB(" 自动刹车预警",10),
    FFW(" 驾驶辅助功能失效",11);

    ADASEnum(String text,int vaule) {
        this.vaule = vaule;
        this.text = text;
    }
    public static  ADASEnum get(int v){
        for (ADASEnum e :values() ) {
            if (e.getVaule()==v){
                return e;
            }
        }
        return null;
    }

    private  int vaule;
    private  String text;

    public int getVaule() {
        return vaule;
    }

    public void setVaule(int vaule) {
        this.vaule = vaule;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }




}
