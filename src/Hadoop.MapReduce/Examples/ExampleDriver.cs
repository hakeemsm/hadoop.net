using System;
using Org.Apache.Hadoop.Examples.Dancing;
using Org.Apache.Hadoop.Examples.PI;
using Org.Apache.Hadoop.Examples.Terasort;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// A description of an example program based on its class and a
	/// human-readable description.
	/// </summary>
	public class ExampleDriver
	{
		public static void Main(string[] argv)
		{
			int exitCode = -1;
			ProgramDriver pgd = new ProgramDriver();
			try
			{
				pgd.AddClass("wordcount", typeof(WordCount), "A map/reduce program that counts the words in the input files."
					);
				pgd.AddClass("wordmean", typeof(WordMean), "A map/reduce program that counts the average length of the words in the input files."
					);
				pgd.AddClass("wordmedian", typeof(WordMedian), "A map/reduce program that counts the median length of the words in the input files."
					);
				pgd.AddClass("wordstandarddeviation", typeof(WordStandardDeviation), "A map/reduce program that counts the standard deviation of the length of the words in the input files."
					);
				pgd.AddClass("aggregatewordcount", typeof(AggregateWordCount), "An Aggregate based map/reduce program that counts the words in the input files."
					);
				pgd.AddClass("aggregatewordhist", typeof(AggregateWordHistogram), "An Aggregate based map/reduce program that computes the histogram of the words in the input files."
					);
				pgd.AddClass("grep", typeof(Grep), "A map/reduce program that counts the matches of a regex in the input."
					);
				pgd.AddClass("randomwriter", typeof(RandomWriter), "A map/reduce program that writes 10GB of random data per node."
					);
				pgd.AddClass("randomtextwriter", typeof(RandomTextWriter), "A map/reduce program that writes 10GB of random textual data per node."
					);
				pgd.AddClass("sort", typeof(Sort), "A map/reduce program that sorts the data written by the random writer."
					);
				pgd.AddClass("pi", typeof(QuasiMonteCarlo), QuasiMonteCarlo.Description);
				pgd.AddClass("bbp", typeof(BaileyBorweinPlouffe), BaileyBorweinPlouffe.Description
					);
				pgd.AddClass("distbbp", typeof(DistBbp), DistBbp.Description);
				pgd.AddClass("pentomino", typeof(DistributedPentomino), "A map/reduce tile laying program to find solutions to pentomino problems."
					);
				pgd.AddClass("secondarysort", typeof(SecondarySort), "An example defining a secondary sort to the reduce."
					);
				pgd.AddClass("sudoku", typeof(Sudoku), "A sudoku solver.");
				pgd.AddClass("join", typeof(Join), "A job that effects a join over sorted, equally partitioned datasets"
					);
				pgd.AddClass("multifilewc", typeof(MultiFileWordCount), "A job that counts words from several files."
					);
				pgd.AddClass("dbcount", typeof(DBCountPageView), "An example job that count the pageview counts from a database."
					);
				pgd.AddClass("teragen", typeof(TeraGen), "Generate data for the terasort");
				pgd.AddClass("terasort", typeof(TeraSort), "Run the terasort");
				pgd.AddClass("teravalidate", typeof(TeraValidate), "Checking results of terasort"
					);
				exitCode = pgd.Run(argv);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			System.Environment.Exit(exitCode);
		}
	}
}
