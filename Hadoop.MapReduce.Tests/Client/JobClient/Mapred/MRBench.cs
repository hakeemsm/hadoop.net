using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Runs a job multiple times and takes average of all runs.</summary>
	public class MRBench : Configured, Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MRBench));

		private static Path BaseDir = new Path(Runtime.GetProperty("test.build.data", "/benchmarks/MRBench"
			));

		private static Path InputDir = new Path(BaseDir, "mr_input");

		private static Path OutputDir = new Path(BaseDir, "mr_output");

		public enum Order
		{
			Random,
			Ascending,
			Descending
		}

		/// <summary>
		/// Takes input format as text lines, runs some processing on it and
		/// writes out data as text again.
		/// </summary>
		public class Map : MapReduceBase, Mapper<WritableComparable, Text, UTF8, UTF8>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Text value, OutputCollector<UTF8, 
				UTF8> output, Reporter reporter)
			{
				string line = value.ToString();
				output.Collect(new UTF8(Process(line)), new UTF8(string.Empty));
			}

			public virtual string Process(string line)
			{
				return line;
			}
		}

		/// <summary>Ignores the key and writes values to the output.</summary>
		public class Reduce : MapReduceBase, Reducer<UTF8, UTF8, UTF8, UTF8>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(UTF8 key, IEnumerator<UTF8> values, OutputCollector<UTF8
				, UTF8> output, Reporter reporter)
			{
				while (values.HasNext())
				{
					output.Collect(key, new UTF8(values.Next().ToString()));
				}
			}
		}

		/// <summary>Generate a text file on the given filesystem with the given path name.</summary>
		/// <remarks>
		/// Generate a text file on the given filesystem with the given path name.
		/// The text file will contain the given number of lines of generated data.
		/// The generated data are string representations of numbers.  Each line
		/// is the same length, which is achieved by padding each number with
		/// an appropriate number of leading '0' (zero) characters.  The order of
		/// generated data is one of ascending, descending, or random.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void GenerateTextFile(FileSystem fs, Path inputFile, long numLines
			, MRBench.Order sortOrder)
		{
			Log.Info("creating control file: " + numLines + " numLines, " + sortOrder + " sortOrder"
				);
			TextWriter output = null;
			try
			{
				output = new TextWriter(fs.Create(inputFile));
				int padding = numLines.ToString().Length;
				switch (sortOrder)
				{
					case MRBench.Order.Random:
					{
						for (long l = 0; l < numLines; l++)
						{
							output.WriteLine(Pad((new Random()).NextLong(), padding));
						}
						break;
					}

					case MRBench.Order.Ascending:
					{
						for (long l_1 = 0; l_1 < numLines; l_1++)
						{
							output.WriteLine(Pad(l_1, padding));
						}
						break;
					}

					case MRBench.Order.Descending:
					{
						for (long l_2 = numLines; l_2 > 0; l_2--)
						{
							output.WriteLine(Pad(l_2, padding));
						}
						break;
					}
				}
			}
			finally
			{
				if (output != null)
				{
					output.Close();
				}
			}
			Log.Info("created control file: " + inputFile);
		}

		/// <summary>
		/// Convert the given number to a string and pad the number with
		/// leading '0' (zero) characters so that the string is exactly
		/// the given length.
		/// </summary>
		private static string Pad(long number, int length)
		{
			string str = number.ToString();
			StringBuilder value = new StringBuilder();
			for (int i = str.Length; i < length; i++)
			{
				value.Append("0");
			}
			value.Append(str);
			return value.ToString();
		}

		/// <summary>Create the job configuration.</summary>
		private JobConf SetupJob(int numMaps, int numReduces, string jarFile)
		{
			JobConf jobConf = new JobConf(GetConf());
			jobConf.SetJarByClass(typeof(MRBench));
			FileInputFormat.AddInputPath(jobConf, InputDir);
			jobConf.SetInputFormat(typeof(TextInputFormat));
			jobConf.SetOutputFormat(typeof(TextOutputFormat));
			jobConf.SetOutputValueClass(typeof(UTF8));
			jobConf.SetMapOutputKeyClass(typeof(UTF8));
			jobConf.SetMapOutputValueClass(typeof(UTF8));
			if (null != jarFile)
			{
				jobConf.SetJar(jarFile);
			}
			jobConf.SetMapperClass(typeof(MRBench.Map));
			jobConf.SetReducerClass(typeof(MRBench.Reduce));
			jobConf.SetNumMapTasks(numMaps);
			jobConf.SetNumReduceTasks(numReduces);
			jobConf.SetBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
			return jobConf;
		}

		/// <summary>Runs a MapReduce task, given number of times.</summary>
		/// <remarks>
		/// Runs a MapReduce task, given number of times. The input to each run
		/// is the same file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private AList<long> RunJobInSequence(JobConf masterJobConf, int numRuns)
		{
			Random rand = new Random();
			AList<long> execTimes = new AList<long>();
			for (int i = 0; i < numRuns; i++)
			{
				// create a new job conf every time, reusing same object does not work 
				JobConf jobConf = new JobConf(masterJobConf);
				// reset the job jar because the copy constructor doesn't
				jobConf.SetJar(masterJobConf.GetJar());
				// give a new random name to output of the mapred tasks
				FileOutputFormat.SetOutputPath(jobConf, new Path(OutputDir, "output_" + rand.Next
					()));
				Log.Info("Running job " + i + ":" + " input=" + FileInputFormat.GetInputPaths(jobConf
					)[0] + " output=" + FileOutputFormat.GetOutputPath(jobConf));
				// run the mapred task now 
				long curTime = Runtime.CurrentTimeMillis();
				JobClient.RunJob(jobConf);
				execTimes.AddItem(Runtime.CurrentTimeMillis() - curTime);
			}
			return execTimes;
		}

		/// <summary>
		/// <pre>
		/// Usage: mrbench
		/// [-baseDir <base DFS path for output/input, default is /benchmarks/MRBench>]
		/// [-jar <local path to job jar file containing Mapper and Reducer implementations, default is current jar file>]
		/// [-numRuns <number of times to run the job, default is 1>]
		/// [-maps <number of maps for each run, default is 2>]
		/// [-reduces <number of reduces for each run, default is 1>]
		/// [-inputLines <number of input lines to generate, default is 1>]
		/// [-inputType <type of input to generate, one of ascending (default), descending, random>]
		/// [-verbose]
		/// </pre>
		/// </summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new MRBench(), args);
			System.Environment.Exit(res);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			string version = "MRBenchmark.0.0.2";
			System.Console.Out.WriteLine(version);
			string usage = "Usage: mrbench " + "[-baseDir <base DFS path for output/input, default is /benchmarks/MRBench>] "
				 + "[-jar <local path to job jar file containing Mapper and Reducer implementations, default is current jar file>] "
				 + "[-numRuns <number of times to run the job, default is 1>] " + "[-maps <number of maps for each run, default is 2>] "
				 + "[-reduces <number of reduces for each run, default is 1>] " + "[-inputLines <number of input lines to generate, default is 1>] "
				 + "[-inputType <type of input to generate, one of ascending (default), descending, random>] "
				 + "[-verbose]";
			string jarFile = null;
			int inputLines = 1;
			int numRuns = 1;
			int numMaps = 2;
			int numReduces = 1;
			bool verbose = false;
			MRBench.Order inputSortOrder = MRBench.Order.Ascending;
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-jar"))
				{
					jarFile = args[++i];
				}
				else
				{
					if (args[i].Equals("-numRuns"))
					{
						numRuns = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-baseDir"))
						{
							BaseDir = new Path(args[++i]);
						}
						else
						{
							if (args[i].Equals("-maps"))
							{
								numMaps = System.Convert.ToInt32(args[++i]);
							}
							else
							{
								if (args[i].Equals("-reduces"))
								{
									numReduces = System.Convert.ToInt32(args[++i]);
								}
								else
								{
									if (args[i].Equals("-inputLines"))
									{
										inputLines = System.Convert.ToInt32(args[++i]);
									}
									else
									{
										if (args[i].Equals("-inputType"))
										{
											string s = args[++i];
											if (Sharpen.Runtime.EqualsIgnoreCase(s, "ascending"))
											{
												inputSortOrder = MRBench.Order.Ascending;
											}
											else
											{
												if (Sharpen.Runtime.EqualsIgnoreCase(s, "descending"))
												{
													inputSortOrder = MRBench.Order.Descending;
												}
												else
												{
													if (Sharpen.Runtime.EqualsIgnoreCase(s, "random"))
													{
														inputSortOrder = MRBench.Order.Random;
													}
													else
													{
														inputSortOrder = null;
													}
												}
											}
										}
										else
										{
											if (args[i].Equals("-verbose"))
											{
												verbose = true;
											}
											else
											{
												System.Console.Error.WriteLine(usage);
												System.Environment.Exit(-1);
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (numRuns < 1 || numMaps < 1 || numReduces < 1 || inputLines < 0 || inputSortOrder
				 == null)
			{
				// verify args
				System.Console.Error.WriteLine(usage);
				return -1;
			}
			JobConf jobConf = SetupJob(numMaps, numReduces, jarFile);
			FileSystem fs = FileSystem.Get(jobConf);
			Path inputFile = new Path(InputDir, "input_" + (new Random()).Next() + ".txt");
			GenerateTextFile(fs, inputFile, inputLines, inputSortOrder);
			// setup test output directory
			fs.Mkdirs(BaseDir);
			AList<long> execTimes = new AList<long>();
			try
			{
				execTimes = RunJobInSequence(jobConf, numRuns);
			}
			finally
			{
				// delete output -- should we really do this?
				fs.Delete(BaseDir, true);
			}
			if (verbose)
			{
				// Print out a report 
				System.Console.Out.WriteLine("Total MapReduce jobs executed: " + numRuns);
				System.Console.Out.WriteLine("Total lines of data per job: " + inputLines);
				System.Console.Out.WriteLine("Maps per job: " + numMaps);
				System.Console.Out.WriteLine("Reduces per job: " + numReduces);
			}
			int i_1 = 0;
			long totalTime = 0;
			foreach (long time in execTimes)
			{
				totalTime += time;
				if (verbose)
				{
					System.Console.Out.WriteLine("Total milliseconds for task: " + (++i_1) + " = " + 
						time);
				}
			}
			long avgTime = totalTime / numRuns;
			System.Console.Out.WriteLine("DataLines\tMaps\tReduces\tAvgTime (milliseconds)");
			System.Console.Out.WriteLine(inputLines + "\t\t" + numMaps + "\t" + numReduces + 
				"\t" + avgTime);
			return 0;
		}
	}
}
