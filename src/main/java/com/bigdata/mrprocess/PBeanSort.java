package com.bigdata.mrprocess;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PBeanSort implements WritableComparable<PBeanSort> {
    private int id;
    private String userName;
    private String passowrd;

    //反序列化时，需要反射调用一个空参构造函数
    //第十六步，mapper全部执行完后，执行空参构造
    public PBeanSort() {
        System.out.println("===>>PBeanSort -- PBeanSort空参");
    }

    public PBeanSort(int id, String userName, String passowrd) {
        System.out.println("===>>PBeanSort -- PBeanSort赋值");
        this.id = id;
        this.userName = userName;
        this.passowrd = passowrd;
    }

    //map/reduce最终输出如果是此bean时，会调用此方法输出
    @Override
    public String toString() {
        System.out.println("===>>PBeanSort -- toString");
        return "PBeanSort{" +
                "id=" + id +
                ", userName='" + userName + '\'' +
                ", passowrd='" + passowrd + '\'' +
                '}';
    }

    public int getId() {
        System.out.println("===>>PBeanSort -- getId");
        return id;
    }

    public void setId(int id) {
        System.out.println("===>>PBeanSort -- setId");
        this.id = id;
    }

    public String getUserName() {
        System.out.println("===>>PBeanSort -- getUserName");
        return userName;
    }

    public void setUserName(String userName) {
        System.out.println("===>>PBeanSort -- setUserName");
        this.userName = userName;
    }

    public String getPassowrd() {
        System.out.println("===>>PBeanSort -- getPassowrd");
        return passowrd;
    }

    public void setPassowrd(String passowrd) {
        System.out.println("===>>PBeanSort -- setPassowrd");
        this.passowrd = passowrd;
    }

    /**
     * 倒序排序
     * 1为正序
     * -1为倒序
     * @param o
     * @return
     */
    @Override
    public int compareTo(PBeanSort o) {
        System.out.println("===>>PBeanSort -- compareTo");
        return id>o.id?-1:1;
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    //第十一步，map逻辑层执行后，进行序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        System.out.println("===>>PBeanSort -- write");
        System.out.println(id+"\t"+userName+"\t"+passowrd);
        dataOutput.write(id);
        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(passowrd);
    }

    /**
     * 反序列化方法
     * 获取的顺序根据write的顺序而定
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        System.out.println("===>>PBeanSort -- readFields");
        id=dataInput.readInt();
//        userName=dataInput.readUTF();
//        passowrd=dataInput.readUTF();
    }
}
