package com.bigdata.hadooprpc.service;

import com.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

public class  MyNameNode implements ClientNamenodeProtocol {

    /**
     * 模拟nameNode的业务方法之一，查询院数据
     * @param path
     * @return
     */
    @Override
    public String getMyData(String path){
        return path+" 3 - {BLk1,BLK2}";
    }
}
