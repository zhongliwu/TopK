package edu.usc.cs.util;

public class TopKRandomGenerator {
    public static float generateFloat(float low, float high) {
        return (float)(Math.random() * (double)(high - low) + (double)low);
    }

    public static int generateInteger(int low, int high) {
        return (int)(Math.round(Math.random() * (double)(high - low)) + (double)low);
    }

    public static String generateName() {
        StringBuilder name = new StringBuilder();
        for(int i = 0; i < 6; i++) {
            if(i == 0) {
                int x1 = generateInteger(65, 90);
                int x2 = generateInteger(97, 122);

                if(Math.random() >= 0.5) name.append((char)x1);
                else name.append((char)x2);
            } else {
                int x1 = generateInteger(65, 90);
                int x2 = generateInteger(97, 122);
                int x3 = generateInteger(48, 57);

                if(Math.random() <= 0.333333) name.append((char)x1);
                else if(Math.random() > 0.333333 && Math.random() <= 0.666666) name.append((char)x2);
                else name.append((char)x3);
            }
        }

        return name.toString();
    }
}