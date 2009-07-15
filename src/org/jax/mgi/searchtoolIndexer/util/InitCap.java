package org.jax.mgi.searchtoolIndexer.util;

/**
 * Utility class that init caps strings, this was copied from an outside source.
 * @author mhall
 * @has A string, that it converts to an initial capped version.
 * @does Iterates through a the character array formed from the passed in string, 
 * converting the initial characters into the uppercase versions of them.
 */

public class InitCap {
    /**
     * Init cap all words in a string passed to this function.  
     * This was being used in enough of the disparate gatherers 
     * that it was moved to the superclass.
     * 
     * This is code taken from another source.
     * 
     * @param in String to be IntiCapped
     * @return String that has been InitCapped
     */

    public static String initCap(String in) {
        if (in == null || in.length() == 0)
            return in;
        
        boolean capitalize = true;
        char[] data = in.toCharArray();
        for (int i = 0; i < data.length; i++) {
            if (data[i] == ' ' || Character.isWhitespace(data[i]))
                capitalize = true;
            else if (capitalize) {
                data[i] = Character.toUpperCase(data[i]);
                capitalize = false;
            } else
                data[i] = Character.toLowerCase(data[i]);
        }
        return new String(data);
    } 

}
