package com.mx;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FileGenerator {

    private static Random random = new Random();

    private static final String FILEPATH = "C:\\Users\\maxu\\Desktop\\f4.nb";

    //生成6个随机字段
    public static void main(String[] args) throws IOException {

        List nameList = new ArrayList() {{
            add("a");
            add("b");
            add("c");
            add("d");
            add("e");
            add("f");
            add("g");
            add("h");
            add("i");
            add("j");
            add("k");
            add("l");
            add("m");
            add("n");
            for (int i = 0; i <= 9; i++) {
                add(i);
            }
        }};

        List idList = new ArrayList() {{
            add(1);
            add(2);
            add(3);
            add(4);
            add(5);
            add(6);
            add(7);
            add(8);
            add(9);
            add(0);
        }};

        List sexList = new ArrayList() {{
            add("男");
            add("女");
            add("");
        }};

        List gradeList = new ArrayList() {{
            for (int i = 0; i <= 100; i++) {
              add(i);
            }
            add("");
        }};

        File tofile = new File(FILEPATH);
        if(!tofile.getParentFile().exists()){
            tofile.mkdirs();
            tofile.createNewFile();
        }
        FileWriter fw = new FileWriter(tofile, true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter pw = new PrintWriter(bw);

        for (int j = 0; j <= 0x7fffffff; j++) {
            //generate one
            String id = "";
            for (int i = 0; i < 32; i++) {
                id += idList.get(random.nextInt(idList.size()));
            }

            String name = "";
            for (int i = 0; i < 16; i++) {
                name += nameList.get(random.nextInt(nameList.size()));
            }

            String sex = (String) sexList.get(random.nextInt(sexList.size()));

            String grade = String.valueOf(gradeList.get(random.nextInt(gradeList.size())));

            String str = id + "\t" + name + "\t" + sex + "\t" + grade;

            pw.println(str);
        }

        pw.close();
        bw.close();
        fw.close();
//        System.out.println(str);
    }


    //输出到指定path文件中
    public static void print(String filePath, String code) throws Exception {
        try {
            File tofile = new File(filePath);
            if(!tofile.exists()){
                tofile.mkdirs();
                tofile.createNewFile();
            }
            FileWriter fw = new FileWriter(tofile, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter pw = new PrintWriter(bw);

            pw.println(code);
            pw.close();
            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
