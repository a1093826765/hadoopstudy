package com.bigdata.hadooprpc.service;

import com.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;
import com.bigdata.hadooprpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;


public class PublishServerUtil {
    public static void main(String[] args) throws Exception {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost").setPort(8888).setProtocol(ClientNamenodeProtocol.class).setInstance(new MyNameNode());
        RPC.Server server = builder.build();
        server.start();

        RPC.Builder builder2 = new RPC.Builder(new Configuration());
        builder2.setBindAddress("localhost")
                .setPort(9999)
                .setProtocol(IUserLoginService.class)
                .setInstance(new UserLoginServiceImpl());

        RPC.Server server2 = builder2.build();
        server2.start();
    }
}
