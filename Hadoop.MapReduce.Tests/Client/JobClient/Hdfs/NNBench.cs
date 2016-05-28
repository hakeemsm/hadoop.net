using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This program executes a specified operation that applies load to
	/// the NameNode.
	/// </summary>
	/// <remarks>
	/// This program executes a specified operation that applies load to
	/// the NameNode.
	/// When run simultaneously on multiple nodes, this program functions
	/// as a stress-test and benchmark for namenode, especially when
	/// the number of bytes written to each file is small.
	/// Valid operations are:
	/// create_write
	/// open_read
	/// rename
	/// delete
	/// NOTE: The open_read, rename and delete operations assume that the files
	/// they operate on are already available. The create_write operation
	/// must be run before running the other operations.
	/// </remarks>
	public class NNBench
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.NNBench"
			);

		protected internal static string ControlDirName = "control";

		protected internal static string OutputDirName = "output";

		protected internal static string DataDirName = "data";

		protected internal const string DefaultResFileName = "NNBench_results.log";

		protected internal const string NnbenchVersion = "NameNode Benchmark 0.4";

		public static string operation = "none";

		public static long numberOfMaps = 1l;

		public static long numberOfReduces = 1l;

		public static long startTime = Runtime.CurrentTimeMillis() + (120 * 1000);

		public static long blockSize = 1l;

		public static int bytesToWrite = 0;

		public static long bytesPerChecksum = 1l;

		public static long numberOfFiles = 1l;

		public static short replicationFactorPerFile = 1;

		public static string baseDir = "/benchmarks/NNBench";

		public static bool readFileAfterOpen = false;

		private const string OpCreateWrite = "create_write";

		private const string OpOpenRead = "open_read";

		private const string OpRename = "rename";

		private const string OpDelete = "delete";

		internal static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss','S"
			);

		private static Configuration config = new Configuration();

		// default is 1
		// default is 1
		// default is 'now' + 2min
		// default is 1
		// default is 0
		// default is 1
		// default is 1
		// default is 1
		// default
		// default is to not read
		// Supported operations
		// To display in the format that matches the NN and DN log format
		// Example: 2007-10-26 00:01:19,853
		/// <summary>Clean up the files before a test run</summary>
		/// <exception cref="System.IO.IOException">on error</exception>
		private static void CleanupBeforeTestrun()
		{
			FileSystem tempFS = FileSystem.Get(config);
			// Delete the data directory only if it is the create/write operation
			if (operation.Equals(OpCreateWrite))
			{
				Log.Info("Deleting data directory");
				tempFS.Delete(new Path(baseDir, DataDirName), true);
			}
			tempFS.Delete(new Path(baseDir, ControlDirName), true);
			tempFS.Delete(new Path(baseDir, OutputDirName), true);
		}

		/// <summary>Create control files before a test run.</summary>
		/// <remarks>
		/// Create control files before a test run.
		/// Number of files created is equal to the number of maps specified
		/// </remarks>
		/// <exception cref="System.IO.IOException">on error</exception>
		private static void CreateControlFiles()
		{
			FileSystem tempFS = FileSystem.Get(config);
			Log.Info("Creating " + numberOfMaps + " control files");
			for (int i = 0; i < numberOfMaps; i++)
			{
				string strFileName = "NNBench_Controlfile_" + i;
				Path filePath = new Path(new Path(baseDir, ControlDirName), strFileName);
				SequenceFile.Writer writer = null;
				try
				{
					writer = SequenceFile.CreateWriter(tempFS, config, filePath, typeof(Text), typeof(
						LongWritable), SequenceFile.CompressionType.None);
					writer.Append(new Text(strFileName), new LongWritable(0l));
				}
				finally
				{
					if (writer != null)
					{
						writer.Close();
					}
				}
			}
		}

		/// <summary>Display version</summary>
		private static void DisplayVersion()
		{
			System.Console.Out.WriteLine(NnbenchVersion);
		}

		/// <summary>Display usage</summary>
		private static void DisplayUsage()
		{
			string usage = "Usage: nnbench <options>\n" + "Options:\n" + "\t-operation <Available operations are "
				 + OpCreateWrite + " " + OpOpenRead + " " + OpRename + " " + OpDelete + ". " + "This option is mandatory>\n"
				 + "\t * NOTE: The open_read, rename and delete operations assume " + "that the files they operate on, are already available. "
				 + "The create_write operation must be run before running the " + "other operations.\n"
				 + "\t-maps <number of maps. default is 1. This is not mandatory>\n" + "\t-reduces <number of reduces. default is 1. This is not mandatory>\n"
				 + "\t-startTime <time to start, given in seconds from the epoch. " + "Make sure this is far enough into the future, so all maps "
				 + "(operations) will start at the same time. " + "default is launch time + 2 mins. This is not mandatory>\n"
				 + "\t-blockSize <Block size in bytes. default is 1. " + "This is not mandatory>\n"
				 + "\t-bytesToWrite <Bytes to write. default is 0. " + "This is not mandatory>\n"
				 + "\t-bytesPerChecksum <Bytes per checksum for the files. default is 1. " + "This is not mandatory>\n"
				 + "\t-numberOfFiles <number of files to create. default is 1. " + "This is not mandatory>\n"
				 + "\t-replicationFactorPerFile <Replication factor for the files." + " default is 1. This is not mandatory>\n"
				 + "\t-baseDir <base DFS path. default is /becnhmarks/NNBench. " + "This is not mandatory>\n"
				 + "\t-readFileAfterOpen <true or false. if true, it reads the file and " + "reports the average time to read. This is valid with the open_read "
				 + "operation. default is false. This is not mandatory>\n" + "\t-help: Display the help statement\n";
			System.Console.Out.WriteLine(usage);
		}

		/// <summary>check for arguments and fail if the values are not specified</summary>
		/// <param name="index">
		/// positional number of an argument in the list of command
		/// line's arguments
		/// </param>
		/// <param name="length">total number of arguments</param>
		public static void CheckArgs(int index, int length)
		{
			if (index == length)
			{
				DisplayUsage();
				System.Environment.Exit(-1);
			}
		}

		/// <summary>Parse input arguments</summary>
		/// <param name="args">array of command line's parameters to be parsed</param>
		public static void ParseInputs(string[] args)
		{
			// If there are no command line arguments, exit
			if (args.Length == 0)
			{
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// Parse command line args
			for (int i = 0; i < args.Length; i++)
			{
				if (args[i].Equals("-operation"))
				{
					operation = args[++i];
				}
				else
				{
					if (args[i].Equals("-maps"))
					{
						CheckArgs(i + 1, args.Length);
						numberOfMaps = long.Parse(args[++i]);
					}
					else
					{
						if (args[i].Equals("-reduces"))
						{
							CheckArgs(i + 1, args.Length);
							numberOfReduces = long.Parse(args[++i]);
						}
						else
						{
							if (args[i].Equals("-startTime"))
							{
								CheckArgs(i + 1, args.Length);
								startTime = long.Parse(args[++i]) * 1000;
							}
							else
							{
								if (args[i].Equals("-blockSize"))
								{
									CheckArgs(i + 1, args.Length);
									blockSize = long.Parse(args[++i]);
								}
								else
								{
									if (args[i].Equals("-bytesToWrite"))
									{
										CheckArgs(i + 1, args.Length);
										bytesToWrite = System.Convert.ToInt32(args[++i]);
									}
									else
									{
										if (args[i].Equals("-bytesPerChecksum"))
										{
											CheckArgs(i + 1, args.Length);
											bytesPerChecksum = long.Parse(args[++i]);
										}
										else
										{
											if (args[i].Equals("-numberOfFiles"))
											{
												CheckArgs(i + 1, args.Length);
												numberOfFiles = long.Parse(args[++i]);
											}
											else
											{
												if (args[i].Equals("-replicationFactorPerFile"))
												{
													CheckArgs(i + 1, args.Length);
													replicationFactorPerFile = short.ParseShort(args[++i]);
												}
												else
												{
													if (args[i].Equals("-baseDir"))
													{
														CheckArgs(i + 1, args.Length);
														baseDir = args[++i];
													}
													else
													{
														if (args[i].Equals("-readFileAfterOpen"))
														{
															CheckArgs(i + 1, args.Length);
															readFileAfterOpen = System.Boolean.Parse(args[++i]);
														}
														else
														{
															if (args[i].Equals("-help"))
															{
																DisplayUsage();
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
						}
					}
				}
			}
			Log.Info("Test Inputs: ");
			Log.Info("           Test Operation: " + operation);
			Log.Info("               Start time: " + sdf.Format(Sharpen.Extensions.CreateDate
				(startTime)));
			Log.Info("           Number of maps: " + numberOfMaps);
			Log.Info("        Number of reduces: " + numberOfReduces);
			Log.Info("               Block Size: " + blockSize);
			Log.Info("           Bytes to write: " + bytesToWrite);
			Log.Info("       Bytes per checksum: " + bytesPerChecksum);
			Log.Info("          Number of files: " + numberOfFiles);
			Log.Info("       Replication factor: " + replicationFactorPerFile);
			Log.Info("                 Base dir: " + baseDir);
			Log.Info("     Read file after open: " + readFileAfterOpen);
			// Set user-defined parameters, so the map method can access the values
			config.Set("test.nnbench.operation", operation);
			config.SetLong("test.nnbench.maps", numberOfMaps);
			config.SetLong("test.nnbench.reduces", numberOfReduces);
			config.SetLong("test.nnbench.starttime", startTime);
			config.SetLong("test.nnbench.blocksize", blockSize);
			config.SetInt("test.nnbench.bytestowrite", bytesToWrite);
			config.SetLong("test.nnbench.bytesperchecksum", bytesPerChecksum);
			config.SetLong("test.nnbench.numberoffiles", numberOfFiles);
			config.SetInt("test.nnbench.replicationfactor", (int)replicationFactorPerFile);
			config.Set("test.nnbench.basedir", baseDir);
			config.SetBoolean("test.nnbench.readFileAfterOpen", readFileAfterOpen);
			config.Set("test.nnbench.datadir.name", DataDirName);
			config.Set("test.nnbench.outputdir.name", OutputDirName);
			config.Set("test.nnbench.controldir.name", ControlDirName);
		}

		/// <summary>Analyze the results</summary>
		/// <exception cref="System.IO.IOException">on error</exception>
		private static void AnalyzeResults()
		{
			FileSystem fs = FileSystem.Get(config);
			Path reduceFile = new Path(new Path(baseDir, OutputDirName), "part-00000");
			DataInputStream @in;
			@in = new DataInputStream(fs.Open(reduceFile));
			BufferedReader lines;
			lines = new BufferedReader(new InputStreamReader(@in));
			long totalTimeAL1 = 0l;
			long totalTimeAL2 = 0l;
			long totalTimeTPmS = 0l;
			long lateMaps = 0l;
			long numOfExceptions = 0l;
			long successfulFileOps = 0l;
			long mapStartTimeTPmS = 0l;
			long mapEndTimeTPmS = 0l;
			string resultTPSLine1 = null;
			string resultTPSLine2 = null;
			string resultALLine1 = null;
			string resultALLine2 = null;
			string line;
			while ((line = lines.ReadLine()) != null)
			{
				StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%;");
				string attr = tokens.NextToken();
				if (attr.EndsWith(":totalTimeAL1"))
				{
					totalTimeAL1 = long.Parse(tokens.NextToken());
				}
				else
				{
					if (attr.EndsWith(":totalTimeAL2"))
					{
						totalTimeAL2 = long.Parse(tokens.NextToken());
					}
					else
					{
						if (attr.EndsWith(":totalTimeTPmS"))
						{
							totalTimeTPmS = long.Parse(tokens.NextToken());
						}
						else
						{
							if (attr.EndsWith(":latemaps"))
							{
								lateMaps = long.Parse(tokens.NextToken());
							}
							else
							{
								if (attr.EndsWith(":numOfExceptions"))
								{
									numOfExceptions = long.Parse(tokens.NextToken());
								}
								else
								{
									if (attr.EndsWith(":successfulFileOps"))
									{
										successfulFileOps = long.Parse(tokens.NextToken());
									}
									else
									{
										if (attr.EndsWith(":mapStartTimeTPmS"))
										{
											mapStartTimeTPmS = long.Parse(tokens.NextToken());
										}
										else
										{
											if (attr.EndsWith(":mapEndTimeTPmS"))
											{
												mapEndTimeTPmS = long.Parse(tokens.NextToken());
											}
										}
									}
								}
							}
						}
					}
				}
			}
			// Average latency is the average time to perform 'n' number of
			// operations, n being the number of files
			double avgLatency1 = (double)totalTimeAL1 / successfulFileOps;
			double avgLatency2 = (double)totalTimeAL2 / successfulFileOps;
			// The time it takes for the longest running map is measured. Using that,
			// cluster transactions per second is calculated. It includes time to 
			// retry any of the failed operations
			double longestMapTimeTPmS = (double)(mapEndTimeTPmS - mapStartTimeTPmS);
			double totalTimeTPS = (longestMapTimeTPmS == 0) ? (1000 * successfulFileOps) : (double
				)(1000 * successfulFileOps) / longestMapTimeTPmS;
			// The time it takes to perform 'n' operations is calculated (in ms),
			// n being the number of files. Using that time, the average execution 
			// time is calculated. It includes time to retry any of the
			// failed operations
			double AverageExecutionTime = (totalTimeTPmS == 0) ? (double)successfulFileOps : 
				(double)totalTimeTPmS / successfulFileOps;
			if (operation.Equals(OpCreateWrite))
			{
				// For create/write/close, it is treated as two transactions,
				// since a file create from a client perspective involves create and close
				resultTPSLine1 = "               TPS: Create/Write/Close: " + (int)(totalTimeTPS 
					* 2);
				resultTPSLine2 = "Avg exec time (ms): Create/Write/Close: " + AverageExecutionTime;
				resultALLine1 = "            Avg Lat (ms): Create/Write: " + avgLatency1;
				resultALLine2 = "                   Avg Lat (ms): Close: " + avgLatency2;
			}
			else
			{
				if (operation.Equals(OpOpenRead))
				{
					resultTPSLine1 = "                        TPS: Open/Read: " + (int)totalTimeTPS;
					resultTPSLine2 = "         Avg Exec time (ms): Open/Read: " + AverageExecutionTime;
					resultALLine1 = "                    Avg Lat (ms): Open: " + avgLatency1;
					if (readFileAfterOpen)
					{
						resultALLine2 = "                  Avg Lat (ms): Read: " + avgLatency2;
					}
				}
				else
				{
					if (operation.Equals(OpRename))
					{
						resultTPSLine1 = "                           TPS: Rename: " + (int)totalTimeTPS;
						resultTPSLine2 = "            Avg Exec time (ms): Rename: " + AverageExecutionTime;
						resultALLine1 = "                  Avg Lat (ms): Rename: " + avgLatency1;
					}
					else
					{
						if (operation.Equals(OpDelete))
						{
							resultTPSLine1 = "                           TPS: Delete: " + (int)totalTimeTPS;
							resultTPSLine2 = "            Avg Exec time (ms): Delete: " + AverageExecutionTime;
							resultALLine1 = "                  Avg Lat (ms): Delete: " + avgLatency1;
						}
					}
				}
			}
			string[] resultLines = new string[] { "-------------- NNBench -------------- : ", 
				"                               Version: " + NnbenchVersion, "                           Date & time: "
				 + sdf.Format(Sharpen.Extensions.CreateDate(Runtime.CurrentTimeMillis())), string.Empty
				, "                        Test Operation: " + operation, "                            Start time: "
				 + sdf.Format(Sharpen.Extensions.CreateDate(startTime)), "                           Maps to run: "
				 + numberOfMaps, "                        Reduces to run: " + numberOfReduces, "                    Block Size (bytes): "
				 + blockSize, "                        Bytes to write: " + bytesToWrite, "                    Bytes per checksum: "
				 + bytesPerChecksum, "                       Number of files: " + numberOfFiles, 
				"                    Replication factor: " + replicationFactorPerFile, "            Successful file operations: "
				 + successfulFileOps, string.Empty, "        # maps that missed the barrier: " +
				 lateMaps, "                          # exceptions: " + numOfExceptions, string.Empty
				, resultTPSLine1, resultTPSLine2, resultALLine1, resultALLine2, string.Empty, "                 RAW DATA: AL Total #1: "
				 + totalTimeAL1, "                 RAW DATA: AL Total #2: " + totalTimeAL2, "              RAW DATA: TPS Total (ms): "
				 + totalTimeTPmS, "       RAW DATA: Longest Map Time (ms): " + longestMapTimeTPmS
				, "                   RAW DATA: Late maps: " + lateMaps, "             RAW DATA: # of exceptions: "
				 + numOfExceptions, string.Empty };
			TextWriter res = new TextWriter(new FileOutputStream(new FilePath(DefaultResFileName
				), true));
			// Write to a file and also dump to log
			for (int i = 0; i < resultLines.Length; i++)
			{
				Log.Info(resultLines[i]);
				res.WriteLine(resultLines[i]);
			}
		}

		/// <summary>Run the test</summary>
		/// <exception cref="System.IO.IOException">on error</exception>
		public static void RunTests()
		{
			config.SetLong("io.bytes.per.checksum", bytesPerChecksum);
			JobConf job = new JobConf(config, typeof(NNBench));
			job.SetJobName("NNBench-" + operation);
			FileInputFormat.SetInputPaths(job, new Path(baseDir, ControlDirName));
			job.SetInputFormat(typeof(SequenceFileInputFormat));
			// Explicitly set number of max map attempts to 1.
			job.SetMaxMapAttempts(1);
			// Explicitly turn off speculative execution
			job.SetSpeculativeExecution(false);
			job.SetMapperClass(typeof(NNBench.NNBenchMapper));
			job.SetReducerClass(typeof(NNBench.NNBenchReducer));
			FileOutputFormat.SetOutputPath(job, new Path(baseDir, OutputDirName));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetNumReduceTasks((int)numberOfReduces);
			JobClient.RunJob(job);
		}

		/// <summary>Validate the inputs</summary>
		public static void ValidateInputs()
		{
			// If it is not one of the four operations, then fail
			if (!operation.Equals(OpCreateWrite) && !operation.Equals(OpOpenRead) && !operation
				.Equals(OpRename) && !operation.Equals(OpDelete))
			{
				System.Console.Error.WriteLine("Error: Unknown operation: " + operation);
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// If number of maps is a negative number, then fail
			// Hadoop allows the number of maps to be 0
			if (numberOfMaps < 0)
			{
				System.Console.Error.WriteLine("Error: Number of maps must be a positive number");
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// If number of reduces is a negative number or 0, then fail
			if (numberOfReduces <= 0)
			{
				System.Console.Error.WriteLine("Error: Number of reduces must be a positive number"
					);
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// If blocksize is a negative number or 0, then fail
			if (blockSize <= 0)
			{
				System.Console.Error.WriteLine("Error: Block size must be a positive number");
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// If bytes to write is a negative number, then fail
			if (bytesToWrite < 0)
			{
				System.Console.Error.WriteLine("Error: Bytes to write must be a positive number");
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// If bytes per checksum is a negative number, then fail
			if (bytesPerChecksum < 0)
			{
				System.Console.Error.WriteLine("Error: Bytes per checksum must be a positive number"
					);
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// If number of files is a negative number, then fail
			if (numberOfFiles < 0)
			{
				System.Console.Error.WriteLine("Error: Number of files must be a positive number"
					);
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// If replication factor is a negative number, then fail
			if (replicationFactorPerFile < 0)
			{
				System.Console.Error.WriteLine("Error: Replication factor must be a positive number"
					);
				DisplayUsage();
				System.Environment.Exit(-1);
			}
			// If block size is not a multiple of bytesperchecksum, fail
			if (blockSize % bytesPerChecksum != 0)
			{
				System.Console.Error.WriteLine("Error: Block Size in bytes must be a multiple of "
					 + "bytes per checksum: ");
				DisplayUsage();
				System.Environment.Exit(-1);
			}
		}

		/// <summary>Main method for running the NNBench benchmarks</summary>
		/// <param name="args">array of command line arguments</param>
		/// <exception cref="System.IO.IOException">indicates a problem with test startup</exception>
		public static void Main(string[] args)
		{
			// Display the application version string
			DisplayVersion();
			// Parse the inputs
			ParseInputs(args);
			// Validate inputs
			ValidateInputs();
			// Clean up files before the test run
			CleanupBeforeTestrun();
			// Create control files before test run
			CreateControlFiles();
			// Run the tests as a map reduce job
			RunTests();
			// Analyze results
			AnalyzeResults();
		}

		/// <summary>Mapper class</summary>
		internal class NNBenchMapper : Configured, Mapper<Text, LongWritable, Text, Text>
		{
			internal FileSystem filesystem = null;

			private string hostName = null;

			internal long numberOfFiles = 1l;

			internal long blkSize = 1l;

			internal short replFactor = 1;

			internal int bytesToWrite = 0;

			internal string baseDir = null;

			internal string dataDirName = null;

			internal string op = null;

			internal bool readFile = false;

			internal readonly int MaxOperationExceptions = 1000;

			internal int numOfExceptions = 0;

			internal long startTimeAL = 0l;

			internal long totalTimeAL1 = 0l;

			internal long totalTimeAL2 = 0l;

			internal long successfulFileOps = 0l;

			/// <summary>Constructor</summary>
			public NNBenchMapper()
			{
			}

			// Data to collect from the operation
			/// <summary>Mapper base implementation</summary>
			public virtual void Configure(JobConf conf)
			{
				SetConf(conf);
				try
				{
					filesystem = FileSystem.Get(conf);
				}
				catch (Exception e)
				{
					throw new RuntimeException("Cannot get file system.", e);
				}
				try
				{
					hostName = Sharpen.Runtime.GetLocalHost().GetHostName();
				}
				catch (Exception e)
				{
					throw new RuntimeException("Error getting hostname", e);
				}
			}

			/// <summary>Mapper base implementation</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			/// <summary>
			/// Returns when the current number of seconds from the epoch equals
			/// the command line argument given by <code>-startTime</code>.
			/// </summary>
			/// <remarks>
			/// Returns when the current number of seconds from the epoch equals
			/// the command line argument given by <code>-startTime</code>.
			/// This allows multiple instances of this program, running on clock
			/// synchronized nodes, to start at roughly the same time.
			/// </remarks>
			/// <returns>
			/// true if the method was able to sleep for <code>-startTime</code>
			/// without interruption; false otherwise
			/// </returns>
			private bool Barrier()
			{
				long startTime = GetConf().GetLong("test.nnbench.starttime", 0l);
				long currentTime = Runtime.CurrentTimeMillis();
				long sleepTime = startTime - currentTime;
				bool retVal = false;
				// If the sleep time is greater than 0, then sleep and return
				if (sleepTime > 0)
				{
					Log.Info("Waiting in barrier for: " + sleepTime + " ms");
					try
					{
						Sharpen.Thread.Sleep(sleepTime);
						retVal = true;
					}
					catch (Exception)
					{
						retVal = false;
					}
				}
				return retVal;
			}

			/// <summary>Map method</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(Text key, LongWritable value, OutputCollector<Text, Text>
				 output, Reporter reporter)
			{
				Configuration conf = filesystem.GetConf();
				numberOfFiles = conf.GetLong("test.nnbench.numberoffiles", 1l);
				blkSize = conf.GetLong("test.nnbench.blocksize", 1l);
				replFactor = (short)(conf.GetInt("test.nnbench.replicationfactor", 1));
				bytesToWrite = conf.GetInt("test.nnbench.bytestowrite", 0);
				baseDir = conf.Get("test.nnbench.basedir");
				dataDirName = conf.Get("test.nnbench.datadir.name");
				op = conf.Get("test.nnbench.operation");
				readFile = conf.GetBoolean("test.nnbench.readFileAfterOpen", false);
				long totalTimeTPmS = 0l;
				long startTimeTPmS = 0l;
				long endTimeTPms = 0l;
				numOfExceptions = 0;
				startTimeAL = 0l;
				totalTimeAL1 = 0l;
				totalTimeAL2 = 0l;
				successfulFileOps = 0l;
				if (Barrier())
				{
					if (op.Equals(OpCreateWrite))
					{
						startTimeTPmS = Runtime.CurrentTimeMillis();
						DoCreateWriteOp("file_" + hostName + "_", reporter);
					}
					else
					{
						if (op.Equals(OpOpenRead))
						{
							startTimeTPmS = Runtime.CurrentTimeMillis();
							DoOpenReadOp("file_" + hostName + "_", reporter);
						}
						else
						{
							if (op.Equals(OpRename))
							{
								startTimeTPmS = Runtime.CurrentTimeMillis();
								DoRenameOp("file_" + hostName + "_", reporter);
							}
							else
							{
								if (op.Equals(OpDelete))
								{
									startTimeTPmS = Runtime.CurrentTimeMillis();
									DoDeleteOp("file_" + hostName + "_", reporter);
								}
							}
						}
					}
					endTimeTPms = Runtime.CurrentTimeMillis();
					totalTimeTPmS = endTimeTPms - startTimeTPmS;
				}
				else
				{
					output.Collect(new Text("l:latemaps"), new Text("1"));
				}
				// collect after the map end time is measured
				output.Collect(new Text("l:totalTimeAL1"), new Text(totalTimeAL1.ToString()));
				output.Collect(new Text("l:totalTimeAL2"), new Text(totalTimeAL2.ToString()));
				output.Collect(new Text("l:numOfExceptions"), new Text(numOfExceptions.ToString()
					));
				output.Collect(new Text("l:successfulFileOps"), new Text(successfulFileOps.ToString
					()));
				output.Collect(new Text("l:totalTimeTPmS"), new Text(totalTimeTPmS.ToString()));
				output.Collect(new Text("min:mapStartTimeTPmS"), new Text(startTimeTPmS.ToString(
					)));
				output.Collect(new Text("max:mapEndTimeTPmS"), new Text(endTimeTPms.ToString()));
			}

			/// <summary>Create and Write operation.</summary>
			/// <param name="name">of the prefix of the putput file to be created</param>
			/// <param name="reporter">
			/// an instanse of (@link Reporter) to be used for
			/// status' updates
			/// </param>
			private void DoCreateWriteOp(string name, Reporter reporter)
			{
				FSDataOutputStream @out;
				byte[] buffer = new byte[bytesToWrite];
				for (long l = 0l; l < numberOfFiles; l++)
				{
					Path filePath = new Path(new Path(baseDir, dataDirName), name + "_" + l);
					bool successfulOp = false;
					while (!successfulOp && numOfExceptions < MaxOperationExceptions)
					{
						try
						{
							// Set up timer for measuring AL (transaction #1)
							startTimeAL = Runtime.CurrentTimeMillis();
							// Create the file
							// Use a buffer size of 512
							@out = filesystem.Create(filePath, true, 512, replFactor, blkSize);
							@out.Write(buffer);
							totalTimeAL1 += (Runtime.CurrentTimeMillis() - startTimeAL);
							// Close the file / file output stream
							// Set up timers for measuring AL (transaction #2)
							startTimeAL = Runtime.CurrentTimeMillis();
							@out.Close();
							totalTimeAL2 += (Runtime.CurrentTimeMillis() - startTimeAL);
							successfulOp = true;
							successfulFileOps++;
							reporter.SetStatus("Finish " + l + " files");
						}
						catch (IOException)
						{
							Log.Info("Exception recorded in op: " + "Create/Write/Close");
							numOfExceptions++;
						}
					}
				}
			}

			/// <summary>Open operation</summary>
			/// <param name="name">of the prefix of the putput file to be read</param>
			/// <param name="reporter">
			/// an instanse of (@link Reporter) to be used for
			/// status' updates
			/// </param>
			private void DoOpenReadOp(string name, Reporter reporter)
			{
				FSDataInputStream input;
				byte[] buffer = new byte[bytesToWrite];
				for (long l = 0l; l < numberOfFiles; l++)
				{
					Path filePath = new Path(new Path(baseDir, dataDirName), name + "_" + l);
					bool successfulOp = false;
					while (!successfulOp && numOfExceptions < MaxOperationExceptions)
					{
						try
						{
							// Set up timer for measuring AL
							startTimeAL = Runtime.CurrentTimeMillis();
							input = filesystem.Open(filePath);
							totalTimeAL1 += (Runtime.CurrentTimeMillis() - startTimeAL);
							// If the file needs to be read (specified at command line)
							if (readFile)
							{
								startTimeAL = Runtime.CurrentTimeMillis();
								input.ReadFully(buffer);
								totalTimeAL2 += (Runtime.CurrentTimeMillis() - startTimeAL);
							}
							input.Close();
							successfulOp = true;
							successfulFileOps++;
							reporter.SetStatus("Finish " + l + " files");
						}
						catch (IOException e)
						{
							Log.Info("Exception recorded in op: OpenRead " + e);
							numOfExceptions++;
						}
					}
				}
			}

			/// <summary>Rename operation</summary>
			/// <param name="name">of prefix of the file to be renamed</param>
			/// <param name="reporter">
			/// an instanse of (@link Reporter) to be used for
			/// status' updates
			/// </param>
			private void DoRenameOp(string name, Reporter reporter)
			{
				for (long l = 0l; l < numberOfFiles; l++)
				{
					Path filePath = new Path(new Path(baseDir, dataDirName), name + "_" + l);
					Path filePathR = new Path(new Path(baseDir, dataDirName), name + "_r_" + l);
					bool successfulOp = false;
					while (!successfulOp && numOfExceptions < MaxOperationExceptions)
					{
						try
						{
							// Set up timer for measuring AL
							startTimeAL = Runtime.CurrentTimeMillis();
							filesystem.Rename(filePath, filePathR);
							totalTimeAL1 += (Runtime.CurrentTimeMillis() - startTimeAL);
							successfulOp = true;
							successfulFileOps++;
							reporter.SetStatus("Finish " + l + " files");
						}
						catch (IOException)
						{
							Log.Info("Exception recorded in op: Rename");
							numOfExceptions++;
						}
					}
				}
			}

			/// <summary>Delete operation</summary>
			/// <param name="name">of prefix of the file to be deleted</param>
			/// <param name="reporter">
			/// an instanse of (@link Reporter) to be used for
			/// status' updates
			/// </param>
			private void DoDeleteOp(string name, Reporter reporter)
			{
				for (long l = 0l; l < numberOfFiles; l++)
				{
					Path filePath = new Path(new Path(baseDir, dataDirName), name + "_" + l);
					bool successfulOp = false;
					while (!successfulOp && numOfExceptions < MaxOperationExceptions)
					{
						try
						{
							// Set up timer for measuring AL
							startTimeAL = Runtime.CurrentTimeMillis();
							filesystem.Delete(filePath, true);
							totalTimeAL1 += (Runtime.CurrentTimeMillis() - startTimeAL);
							successfulOp = true;
							successfulFileOps++;
							reporter.SetStatus("Finish " + l + " files");
						}
						catch (IOException)
						{
							Log.Info("Exception in recorded op: Delete");
							numOfExceptions++;
						}
					}
				}
			}
		}

		/// <summary>Reducer class</summary>
		internal class NNBenchReducer : MapReduceBase, Reducer<Text, Text, Text, Text>
		{
			protected internal string hostName;

			public NNBenchReducer()
			{
				Log.Info("Starting NNBenchReducer !!!");
				try
				{
					hostName = Sharpen.Runtime.GetLocalHost().GetHostName();
				}
				catch (Exception)
				{
					hostName = "localhost";
				}
				Log.Info("Starting NNBenchReducer on " + hostName);
			}

			/// <summary>Reduce method</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(Text key, IEnumerator<Text> values, OutputCollector<Text
				, Text> output, Reporter reporter)
			{
				string field = key.ToString();
				reporter.SetStatus("starting " + field + " ::host = " + hostName);
				// sum long values
				if (field.StartsWith("l:"))
				{
					long lSum = 0;
					while (values.HasNext())
					{
						lSum += long.Parse(values.Next().ToString());
					}
					output.Collect(key, new Text(lSum.ToString()));
				}
				if (field.StartsWith("min:"))
				{
					long minVal = -1;
					while (values.HasNext())
					{
						long value = long.Parse(values.Next().ToString());
						if (minVal == -1)
						{
							minVal = value;
						}
						else
						{
							if (value != 0 && value < minVal)
							{
								minVal = value;
							}
						}
					}
					output.Collect(key, new Text(minVal.ToString()));
				}
				if (field.StartsWith("max:"))
				{
					long maxVal = -1;
					while (values.HasNext())
					{
						long value = long.Parse(values.Next().ToString());
						if (maxVal == -1)
						{
							maxVal = value;
						}
						else
						{
							if (value > maxVal)
							{
								maxVal = value;
							}
						}
					}
					output.Collect(key, new Text(maxVal.ToString()));
				}
				reporter.SetStatus("finished " + field + " ::host = " + hostName);
			}
		}
	}
}
