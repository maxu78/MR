package com.mx.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private String id;
    private String name;
    private String sex;
    private String grade;

    @Override
    public int compareTo(SortBean o) {

        //先按性别排序
        int hs1 = this.sex == null ? 0 : this.sex.hashCode();
        int hs2 = o.sex == null ? 0 : o.sex.hashCode();

        if (hs1 == hs2) {
            //再比较grade
            int hg1 = this.grade == null ? 0 : this.grade.hashCode();
            int hg2 = o.grade == null ? 0 : o.grade.hashCode();
            return hg1 < hg2 ? -1 : 1;
        } else {
            return hs1 < hs2 ? -1 : 1;
        }


    }

    public long convert(String str) {
        long result = 0L;
        try {
            result = Long.parseLong(str);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
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
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.sex = in.readUTF();
        this.grade = in.readUTF();

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

    public void clear() {
        this.name = null;
        this.grade = null;
        this.sex = null;
        this.id = null;
    }

    @Override
    public String toString() {
        return new StringBuffer(id).append("\t").append(name).append("\t")
                .append(sex).append("\t").append(grade).toString();
    }
}
