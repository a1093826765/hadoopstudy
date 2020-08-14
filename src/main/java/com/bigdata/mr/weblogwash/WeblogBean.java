package com.bigdata.mr.weblogwash;

public class WeblogBean {
    private String remote_addr; //记录客户端ip地址
    private String remote_user; //记录客户端用户名称，忽略属性"-"
    private String time_local; //记录访问时间与时区
    private String request; //记录请求的url与http协议
    private String status; //记录请求状态 code
    private String body_bytes_sent; //记录发送给客户端主体内容的大小
    private String http_referer; //记录从页面访问进来的链接
    private String http_user_agent; //记录客户端浏览器的相关信息

    //判断数据是否合法（js,jpg,png等这些我们不需要的数据就判断为不合法）
    private boolean valid=true;

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    @Override
    public String toString() {
        return "weblogBean{" +
                "remote_addr='" + remote_addr + '\'' +
                ", remote_user='" + remote_user + '\'' +
                ", time_local='" + time_local + '\'' +
                ", request='" + request + '\'' +
                ", status='" + status + '\'' +
                ", body_bytes_sent='" + body_bytes_sent + '\'' +
                ", http_referer='" + http_referer + '\'' +
                ", http_user_agent='" + http_user_agent + '\'' +
                ", valid=" + valid +
                '}';
    }
}
