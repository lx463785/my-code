package com.ymhx.dataplatform.kafka.domain;

public enum ADASNewEnum {


    FCWANDUFCW("前向碰撞",1),
    LDWADNLDWR(" 车道偏移报警",2),
    HMW(" 车距检测",3),
    PCW(" 行人碰撞",4),
    ACC(" 频繁变道报警",5),
    TSR(" 限速提示",6),
    ODA(" 障碍物检测警报",7),
    FFW(" 驾驶辅助功能失效",8),
    Road_Risking(" 自动刹车预警",9),
    Active_Capture(" 主动抓拍时间",10),
    FCW(" 前碰撞",11),
    UFCW(" 低速碰撞",12),
    LDW(" 车道左偏离",13),
    LDWR(" 车道右偏离",14);

    ADASNewEnum(String text, int vaule) {
        this.vaule = vaule;
        this.text = text;
    }
    public static ADASNewEnum get(int v){
        for (ADASNewEnum e :values() ) {
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
