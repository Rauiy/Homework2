import org.apache.avro.generic.GenericData;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.*;

/**
 * Created by stevenzhou on 2016-11-15.
 */

public class Apriori {



    public static Map aprioriPass2(JavaRDD <String> large, final Map filter, int support, int n){

        JavaRDD sLarge = large.flatMap((FlatMapFunction<String, ArrayList<String>>) s -> {

                    String[] items = s.split(" ");
                    Arrays.sort(items);

                    if(items.length < n)
                        return new ArrayList<ArrayList<String>>();

                    if(n >= 7)
                        System.out.println("Generating set of " + n);

                    ArrayList<ArrayList<String>> subsets = generateSubsets(items, n);

                    if(n >= 7)
                        System.out.println("Filtering set of " + n);
                    subsets = filter(subsets, filter, n-1);


                    //System.out.println(subsets.toString());
                    return subsets;
                }
        );


        Map<ArrayList, Long> m = sLarge.countByValue();
        Map<ArrayList, Long> filtered = new HashMap<>();
        for(Map.Entry e: m.entrySet())
            if((long)e.getValue() >= support)
                filtered.put((ArrayList) e.getKey(), (long)e.getValue());

        System.out.println(m.size());
        System.out.println(filtered.size());
        System.out.println(filtered.toString());

        return filtered;
    }

    public static ArrayList generateSubsets(String[] arr, int n)
    {
        ArrayList<ArrayList<String>> subsets = new ArrayList<>();
        //System.out.println("Generating subsets of " + n);
        for(int i = 0; i < arr.length-n+1; i++){
            generateSubset(subsets, new ArrayList<>(), arr, i, n);
        }


        //System.out.println(subsets.toString());
        return subsets;
    }

    public static void generateSubset(ArrayList subsets, ArrayList<String> prev, String[] arr, int i, int n){
        ArrayList<String> tmp = new ArrayList<>();
        for(String s: prev)
            tmp.add(s);

        tmp.add(arr[i]);

        if(tmp.size() == n) {
            subsets.add(tmp);
            return;
        }

        for(int j = i+1; j < arr.length; j++){

            generateSubset(subsets, tmp, arr, j, n);
        }

    }

    public static ArrayList filter(ArrayList<ArrayList<String>> subsets, Map filter, int n){
        ArrayList<ArrayList<String>> filtered = new ArrayList<>();
        boolean exists;
        for (ArrayList<String> subset: subsets) {
            //System.out.println(subsets);
            // Creating the subsets of size n to check if they exists
            ArrayList<ArrayList<String>> subs = generateSubsets(subset.toArray(new String[0]), n);

            // Default is that they exists, say otherwise
            exists = true;

            for(ArrayList sub: subs){
                if(!filter.containsKey(sub)) {
                    // This subset of the subset doesn't exist change flag and break
                    exists = false;
                    break;
                }
            }

            // If all subsets of subset exists add it to the okay list
            if(exists)
                filtered.add(subset);
        }

        return filtered;
    }

    public static JavaRDD prepare(JavaRDD<String> large){
        JavaRDD sLarge = large.map(s-> {
            String[] arr = s.split(" ");
            ArrayList<String> strings = new ArrayList<>();
            for(String str: arr) {
                strings.add(str);
            }
            return strings;
        });

        return  sLarge;
    }

    public  static JavaPairRDD aprioriPass1 (JavaRDD<ArrayList<String>> large, JavaSparkContext sc ,int support )
    {
        JavaRDD flatten = large.flatMap(s -> s);
        JavaPairRDD<String,Integer> sLarge = flatten.mapToPair(e -> new Tuple2<>(e,1));
        //sLarge = sLarge.flatMap( s -> s)
        JavaPairRDD<String, Integer> reduce = sLarge.reduceByKey((a,b) -> a+b);
        reduce = reduce.filter(a -> (a._2()>support));

        return reduce;
    }

    public static void main (String[] args)
    {

        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String datafile = "T10I4D100K.dat";

        JavaRDD <String>large = sc.textFile(datafile);

        Apriori apr = new Apriori();
        //apr.aprioriPass1(large,500);

        //Map second = apr.aprioriPass2(large,apr.aprioriPass1(large,500),500, 2);
        //apr.aprioriPass2(large, second, 500, 3);

        JavaRDD source = prepare(large);

        //JavaRDD<String> x = sc.parallelize(Arrays.asList("a", "b", "b","b", "c","c","c", "d"), 3);

        //System.out.println(x.collect());

        JavaPairRDD prev = aprioriPass1(source, sc,500);

        System.out.println(prev.collectAsMap());


        /*
        Map current = prev;

        for(int i= 2; prev.size() >= 2; i++){
            System.out.println("Starting with sets of " + i);
            current = aprioriPass2(large, prev, 500, i);
            prev = current;
        }
*/
        //sLarge.collect().forEach(x->System.out.println(x));
    }

}
