package com.mx.mr.partition;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionBean implements WritableComparable<PartitionBean> {

    private String id;
    private String name;
    private String sex;
    private String grade;

    public void clear() {
        id = null;
        name = null;
        sex = null;
        grade = null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade;
    }

    @Override
    public int compareTo(PartitionBean o) {
        int h1 = sex == null ? 0 : sex.hashCode();
        int h2 = o.sex == null ? 0 : o.sex.hashCode();
        return h1 <= h2 ? -1 : 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(name);
        out.writeUTF(sex);
        out.writeUTF(grade);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        name = in.readUTF();
        sex = in.readUTF();
        grade = in.readUTF();
    }

    @Override
    public String toString() {
        return id + "\t" + name + "\t" + sex + "\t" + grade;
    }
}
