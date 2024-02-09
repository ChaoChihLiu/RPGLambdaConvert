package com.cpbpc.rpgv2;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SpeechSynthesis {
    // This example requires environment variables named "SPEECH_KEY" and "SPEECH_REGION"
    private static String speechKey = "";
    private static String speechRegion = "southeastasia";

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {

        // XML content to send in the POST request
        String xmlContent = IOUtils.toString(new FileReader(new File("/Users/liuchaochih/Documents/GitHub/RPGLambdaConvert/src/main/resources/azuretts.xml")));

        // URL of the endpoint to send the request to
        String url = "https://southeastasia.tts.speech.microsoft.com/cognitiveservices/v1";

        // Create an HttpClient
        HttpClient httpClient = HttpClient.newHttpClient();

        // Create a POST request with the XML content and headers
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("Ocp-Apim-Subscription-Key", "")
                .header("Content-Type", "application/ssml+xml")
                .header("X-Microsoft-OutputFormat", "audio-48khz-192kbitrate-mono-mp3")
                .header("User-Agent", "curl")
                // Add any additional headers as needed
                .POST(HttpRequest.BodyPublishers.ofString(xmlContent))
                .build();

        // Send the request and retrieve the response
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        // Check if the response is successful
        if (response.statusCode() == 200) {
            // Save the MP3 file from the response body
            Files.write(Paths.get("testoutput.mp3"), response.body());
            System.out.println("MP3 file saved successfully.");
        } else {
            System.out.println("Failed to retrieve MP3 file. Response code: " + response.statusCode());
        }

    }

    private static void saveToMp3(byte[] audioData) throws IOException {

        String outputFilePath = "output.mp3"; // Output file path
        Path path = Path.of(outputFilePath);
        Files.write(path, audioData);

    }
}