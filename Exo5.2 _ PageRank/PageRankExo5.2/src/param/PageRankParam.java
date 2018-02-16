package param;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;


public class PageRankParam {
	
	// utility attributes
    public static Set<String> NODES = new HashSet<String>();
    public static String LINKS_SEPARATOR = "|";
    
    // configuration values
    public static Double DAMPING = 0.85;
    public static int nbIterations = 5;

    
    

}
