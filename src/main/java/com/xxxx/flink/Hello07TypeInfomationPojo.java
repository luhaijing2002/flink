package com.xxxx.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello07TypeInfomationPojo {
    public static void main(String[] args) throws Exception {

        //1.环境创建
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        //2.souce
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);


        //3.transformation
        //amdin-123456  => User(admin,123456)
        SingleOutputStreamOperator<User> streamOperator = dataStreamSource.map(line -> {
            String[] split = line.split("-");
            return new User(split[0], Integer.parseInt(split[1]));
        });


        //4.sink
        streamOperator.print();


        //5.执行
        env.execute();


    }
}

class User implements Serializable {
    private String name;
    private int age;


    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public User() {
    }

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return age == user.age && Objects.equals(name, user.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age);
    }



}
