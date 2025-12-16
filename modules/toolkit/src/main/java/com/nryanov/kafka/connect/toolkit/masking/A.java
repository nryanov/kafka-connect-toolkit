package com.nryanov.kafka.connect.toolkit.masking;

public class A {
    public static void main(String[] args) {
//        var input = "Hi, my card is 4111 1111 1111 1111 maybe you should not store that in your database!";
//        var input = "4111 1111 1111 1111";
        var input = "lalal 4111 1111 1111 1111 AND 4833 1200 3412 3456 OR 378282246310005";

        var sanitizer = new CardMaskingService();
        var output = sanitizer.maskCards(input);

        System.out.println(output);
    }
}
