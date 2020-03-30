package com.ymhx.dataplatform.kafka.domain;

public class ResponseMes<T> {

    private  String code;
    private  String message;
    private  T data;
    private  boolean flag=false;

    private  static  String successCode="000";
    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public ResponseMes() {
    }

    public ResponseMes(boolean flag,T data) {
        this.code=successCode;
        this.message="OK";
        this.data = data;
        this.flag = flag;
    }

    public ResponseMes(boolean flag,String code, String message) {
        this.code = code;
        this.message = message;
        this.flag = flag;
    }
}
