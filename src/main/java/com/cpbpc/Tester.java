package com.cpbpc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tester {

    public static void main(String args[]){

        String input = "<speak><prosody rate='80%' volume='loud'>And Jesus said unto them, Can the children of the bridechamber mourn, as long as the bridegroom is with them? but the days will come, when the bridegroom shall be taken from them, and then shall they fast.</prosody></speak>";

        String regex = "(?<!<phoneme[^>]\\*>)\\b(bridechamber)\\b(?!<\\/phoneme>)";

        // Compile the pattern
        Pattern pattern = Pattern.compile(regex);

        // Create a matcher
        Matcher matcher = pattern.matcher(input);

        // Find and output matches
        while (matcher.find()) {
            System.out.println("Match: " + matcher.group());
        }
        
    }

}
