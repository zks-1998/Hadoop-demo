package com.gzhu.put;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
// 1.实现 DBWritable, Writable
public class MyDBWritable implements DBWritable, Writable {
    // 数据库的写入字段
    private Long id;
    private String name;
    private Long age;
    // 写出字段
    private Long ag;
    private Long count;
    // 2.反序列化所需要的空参构造
    public MyDBWritable(){}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getAge() {
        return age;
    }

    public void setAge(Long age) {
        this.age = age;
    }

    public Long getAg() {
        return ag;
    }

    public void setAg(Long ag) {
        this.ag = ag;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    // 3.序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeUTF(name);
        dataOutput.writeLong(age);
    }
    // 4.反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readLong();
        this.name = dataInput.readUTF();
        this.age = dataInput.readLong();
    }

    // 5.从DB读取
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getLong(1);
        name = resultSet.getString(2);
        age = resultSet.getLong(3);
    }
    // 6.写入数据库
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setLong(1,ag);
        preparedStatement.setLong(2,count);
    }

}
