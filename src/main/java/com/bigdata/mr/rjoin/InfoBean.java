package com.bigdata.mr.rjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements Writable {

    private int order_id;
    private String dateString;
    private String p_id;
    private int amount;
    private String p_name;
    private int category_id;
    private float price;

    //flag=0表示这个对象封装订单表记录
    //flag=1表示这个对象封装产品信息记录
    private String flag;

    public InfoBean() {
    }

    public void set(int order_id, String dateString, String p_id, int amount, String p_name, int category_id, float price, String flag) {
        this.order_id = order_id;
        this.dateString = dateString;
        this.p_id = p_id;
        this.amount = amount;
        this.p_name = p_name;
        this.category_id = category_id;
        this.price = price;
        this.flag=flag;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getP_name() {
        return p_name;
    }

    public void setP_name(String p_name) {
        this.p_name = p_name;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return
                "order_id=" + order_id +
                ", dateString='" + dateString + '\'' +
                ", p_id=" + p_id +
                ", amount=" + amount +
                ", p_name='" + p_name + '\'' +
                ", category_id=" + category_id +
                ", price=" + price +
                ", flag='" + flag + '\'' ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeUTF(dateString);
        dataOutput.writeUTF(p_id);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(p_name);
        dataOutput.writeInt(category_id);
        dataOutput.writeFloat(price);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        order_id=dataInput.readInt();
        dateString=dataInput.readUTF();
        p_id=dataInput.readUTF();
        amount=dataInput.readInt();
        p_name=dataInput.readUTF();
        category_id=dataInput.readInt();
        price=dataInput.readFloat();
    }
}
