package mapred.catrank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CatParsingMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final Pattern wikiLinksPattern = Pattern.compile("\\[\\[Category:.+?\\]\\]");
    
    /**
     * key - KEYIN 
     * value - VALUEIN
     */
    protected void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
    	
        String[] titleAndText = parseTitleAndText(value);
        
        String pageString = titleAndText[0];
        
        if(notValidPage(pageString)) return;
        
        Text page = new Text(pageString.replace(' ', '_'));

        Matcher matcher = wikiLinksPattern.matcher(titleAndText[1]);
        
        //Loop through the matched links in [CONTENT]
        while (matcher.find()) {
            String cat = matcher.group();

            //Filter only categories.
            //[[Category:realCat]]
            cat = getCat(cat);
            if(cat == null || cat.isEmpty()) 
                continue;
            
            // add valid otherPages to the map.
            context.write(page, new Text(cat));
        }
    }
       
    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String getCat(String aCat){
                
        //int start = 2;
	int ColonPosition = aCat.indexOf(":");
        int endCat = aCat.indexOf("]");
        
        aCat = aCat.substring(ColonPosition+1, endCat);

        aCat = aCat.replaceAll("\\s", "_");
        aCat = aCat.replaceAll(",", "");
        aCat = sweetify(aCat);
        
        return aCat;
    }
    
    /**
     * replace &amp; chars to & in url
     * 
     * @param aLinkText
     * @return
     */
    private String sweetify(String aLinkText) {
        if(aLinkText.contains("&amp;"))
            return aLinkText.replace("&amp;", "&");

        return aLinkText;
    }

    /**
     * Parse title and Text from given text 
     * 
     * @param value
     * @return - String[0] = <title>[TITLE]</title>
     * 			 String[1] = <text>[CONTENT]</text>
     *   !! without the <tags>
     * @throws CharacterCodingException
     */
    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

}
