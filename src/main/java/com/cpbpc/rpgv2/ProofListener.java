package com.cpbpc.rpgv2;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.util.StringInputStream;
import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;
import com.github.houbb.opencc4j.util.ZhConverterUtil;
import com.google.gson.Gson;
import net.sourceforge.pinyin4j.PinyinHelper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProofListener implements RequestHandler<S3Event, Void> {

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    private static final Pattern object_key_pattern = Pattern.compile("\\d{4,4}_\\d{2,2}");
    @Override
    public Void handleRequest(S3Event s3event, Context context) {

        for (S3Event.S3EventNotificationRecord record : s3event.getRecords()) {
            String bucketName = URLDecoder.decode(record.getS3().getBucket().getName());
            String objectKey = URLDecoder.decode(record.getS3().getObject().getKey());
            System.out.println("bucket name: " + bucketName);
            System.out.println("object key: " + objectKey);

            Matcher matcher = object_key_pattern.matcher(objectKey);
            if( !matcher.find() ){
                System.out.println(objectKey + " is not target, skip");
                return null;
            }

            GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(bucketName, objectKey);
            GetObjectTaggingResult getObjectTaggingResponse = s3Client.getObjectTagging(getObjectTaggingRequest);

            Map<String, String> tags = new HashMap<>();
            getObjectTaggingResponse.getTagSet().forEach(tag -> {
                tags.put(tag.getKey(), tag.getValue());
            });
            if( tags.isEmpty() ){
                continue;
            }
            if( !tags.containsKey("pl_script") || StringUtils.isEmpty(tags.get("pl_script")) ){
                continue;
            }
            String pl_script = URLDecoder.decode(tags.get("pl_script"));
            String pl_script_bucket = URLDecoder.decode(tags.get("pl_script_bucket"));

            //load audio first
            String audioFilePath = downloadS3Object( bucketName, objectKey );
            //load pl script
            String plFilePath = downloadS3Object( pl_script_bucket, pl_script );
            String script = "";
            try {
                script = IOUtils.toString(new FileReader(plFilePath));
            } catch (IOException e) {
                e.printStackTrace();
            }

            if( StringUtils.isEmpty(script) ){
                System.out.println("cannot read script " + plFilePath);
                return null;
            }

            Gson gson = new Gson();
            Map result = gson.fromJson(speechToText(audioFilePath), HashMap.class);
            String fromAI = ZhConverterUtil.toSimple(String.valueOf(result.get("text")));
            fromAI = removeMultiSpace(removePunctuation(fromAI));
            fromAI = fromAI.replaceAll("圣经 经文第", "圣经经文第");

            script = toChineseNumber(ZhConverterUtil.toSimple(
                    removeMultiSpace(
                            removePunctuation(
                                    removeLinkBreak(
                                            removeHtmlTag(script, ""))))));
            StringBuffer buffer = new StringBuffer();
            buffer.append("original: " + script).append(System.lineSeparator());
            buffer.append("from AI : " + fromAI).append(System.lineSeparator());
            buffer.append(System.lineSeparator());

            Patch<String> patch = DiffUtils.diff(Arrays.asList(StringUtils.split(script, " ")), Arrays.asList(StringUtils.split(fromAI, " ")));
            for (AbstractDelta<String> delta : patch.getDeltas()) {
                String oringal = StringUtils.remove(removePunctuation(StringUtils.join(delta.getSource().getLines())), " ");
                String aiRead =  StringUtils.remove(removePunctuation(StringUtils.join(delta.getTarget().getLines())), " ");

                if( StringUtils.equals(convertToPinyin(oringal), convertToPinyin(aiRead)) ){
                    continue;
                }
                
                buffer.append("original: " + StringUtils.join(delta.getSource().getLines())).append(System.lineSeparator());
                buffer.append("from AI : " + StringUtils.join(delta.getTarget().getLines())).append(System.lineSeparator());
                buffer.append(System.lineSeparator());
            }

            saveToS3(buffer.toString(), bucketName, pl_script.replace("pl", "txt"));
        }

        return null;
    }

    private void saveToS3(String content, String bucketName, String objectKey) {
        try {
            InputStream inputStream = new StringInputStream(content);
            // Upload the file to S3
            ObjectMetadata metadata = new ObjectMetadata();
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, inputStream, metadata);

            putObjectRequest.setStorageClass(StorageClass.IntelligentTiering);

            s3Client.putObject(putObjectRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String convertToPinyin(String chineseText) {
        StringBuilder pinyinBuilder = new StringBuilder();

        for (char c : chineseText.toCharArray()) {
            if (Character.toString(c).matches("[\\u4E00-\\u9FA5]+")) {
                String[] pinyinArray = PinyinHelper.toHanyuPinyinStringArray(c);
                if (pinyinArray != null && pinyinArray.length > 0) {
                    pinyinBuilder.append(pinyinArray[0]); // Take the first Pinyin if there are multiple
                } else {
                    pinyinBuilder.append(c); // If Pinyin conversion is not available, keep the original character
                }
            } else {
                pinyinBuilder.append(c); // Keep non-Chinese characters as is
            }
        }

        return pinyinBuilder.toString();
    }

    private String removeHtmlTag(String input){
        return removeHtmlTag(input," ");
    }

    private String removeHtmlTag(String input, String replacement){
        if (org.apache.commons.lang3.StringUtils.isEmpty(input)) {
            return "";
        }

        return input.replaceAll("<[^>]*>|&nbsp;|&zwnj;|&raquo;|&laquo;|&gt;", replacement);
//                .replaceAll("&nbsp;", " ")  ;
    }

    private Pattern line_break_pattern = Pattern.compile("[\\n|\\r\\n]");
    public String removeLinkBreak(String text) {
        Matcher matcher = line_break_pattern.matcher(text);
        String result = matcher.replaceAll(" ");
        return result;
    }

    private String toChineseNumber(int i) {
        Locale chineseNumbers = new Locale("C@numbers=hans");
        com.ibm.icu.text.NumberFormat formatter =
                com.ibm.icu.text.NumberFormat.getInstance(chineseNumbers);

        return formatter.format(i);
    }

    private String toChineseNumber(String text){
        if(StringUtils.isEmpty(text) ){
            return "";
        }

        String result = "";
        String temp = "";
        for( char c : text.toCharArray() ){
            if( !NumberUtils.isCreatable(String.valueOf(c)) ){
                if( !StringUtils.isEmpty(temp) ){
                    result += toChineseNumber(Integer.valueOf(temp));
                    temp = "";
                }
                result += c;
                continue;
            }

            temp += c;
        }


        return result;
    }

    private Pattern punctuation_pattern = Pattern.compile("[\\p{P}]");
    public String removePunctuation(String text) {
        return removePunctuation(text, " ");
    }
    private String removePunctuation(String text, String replacement) {
        Matcher matcher = punctuation_pattern.matcher(text);
        String result = matcher.replaceAll(replacement);
        return result;
    }

    private Pattern multi_space_pattern = Pattern.compile("\\s+");
    private String removeMultiSpace(String input) {
        Matcher matcher = multi_space_pattern.matcher(input);
        String result = matcher.replaceAll(" ");
        return result;
    }
    
    private String downloadS3Object(String bucketName, String objectKey){

        String[] inputs = objectKey.split("/");
        String folder = "";
        for( int i = 0; i<inputs.length-1; i++ ){
            folder += inputs[i]+"/";
        }

        String localFilePath = "/tmp/"+objectKey;
        String folderPath = "/tmp/"+folder;
        System.out.println("folder path " + folderPath);
        System.out.println("audio path " + localFilePath);
        try {
            File dir = new File( folderPath );
            if( !dir.exists() ){
                dir.mkdirs();
            }
            File audioFile = new File(localFilePath);
            if( !audioFile.exists() ){
                audioFile.createNewFile();
            }

            S3Object s3Object = s3Client.getObject(bucketName, objectKey);

            FileOutputStream fos = new FileOutputStream(localFilePath);
            com.amazonaws.util.IOUtils.copy(s3Object.getObjectContent(), fos);
            System.out.println("Object downloaded successfully to: " + localFilePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return localFilePath;
    }

    private String speechToText(String filePath) {
        try {
            // Replace 'YOUR_API_KEY' with your OpenAI API key
            String openai_api_key = System.getenv("openai_api_key");
            String openai_api_url = System.getenv("openai_api_url");
            
            final HttpPost httppost = new HttpPost(openai_api_url);
            httppost.addHeader("Authorization", "Bearer "+openai_api_key);
            httppost.addHeader("Accept", "application/json");

            final MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.setContentType(ContentType.MULTIPART_FORM_DATA);
            final File file = new File(filePath);
            builder.addTextBody("model", "whisper-1");
            builder.addTextBody("language", "zh");
            builder.addTextBody("response_format", "json");
            builder.addTextBody("temperature", "0");
            builder.addPart("file", new FileBody(file));
            final HttpEntity entity = builder.build();
            httppost.setEntity(entity);
            HttpClient client = HttpClientBuilder.create().build();
            final HttpResponse response = client.execute(httppost);

            if( response.getStatusLine().getStatusCode() == 429 ){
                Thread.sleep(20*1000);
                return speechToText(filePath);
            }

            String result =  IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
            System.out.println(result);
            return result;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
