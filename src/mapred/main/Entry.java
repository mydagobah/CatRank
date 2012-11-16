package mapred.main;

import mapred.util.SimpleParser;

/**
 * This is the main program hadoop will start from and do following:
 * 1. check program name
 * 2. calculate the runtime
 * Usage:
 * hadoop jar 18645-proj4-0.1-latest.jar -program catrank -input data/ -output results -tmpdir tmp
 */
public class Entry {
	public static void main(String args[]) throws Exception  {
		SimpleParser parser = new SimpleParser(args);
		String program = parser.get("program");
		
		System.out.println("Running program " + program + "..");

		long start = System.currentTimeMillis();

		if (program.equals("catrank"))
			mapred.catrank.Driver.main(args);
		
		else {
			System.out.println("Unknown program!");
			System.exit(1);
		}		

		long end = System.currentTimeMillis();

		System.out.println(String.format("Runtime for program %s: %d ms", program,
				end - start));
	}
}
