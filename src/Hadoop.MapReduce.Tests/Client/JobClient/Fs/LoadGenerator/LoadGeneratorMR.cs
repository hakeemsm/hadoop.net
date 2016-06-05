using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.LoadGenerator
{
	/// <summary>
	/// The load generator is a tool for testing NameNode behavior under
	/// different client loads.
	/// </summary>
	/// <remarks>
	/// The load generator is a tool for testing NameNode behavior under
	/// different client loads.
	/// The main code is in HadoopCommon, @LoadGenerator. This class, LoadGeneratorMR
	/// lets you run that LoadGenerator as a MapReduce job.
	/// The synopsis of the command is
	/// java LoadGeneratorMR
	/// -mr <numMapJobs> <outputDir> : results in outputDir/Results
	/// the rest of the args are the same as the original LoadGenerator.
	/// </remarks>
	public class LoadGeneratorMR : Org.Apache.Hadoop.FS.LoadGenerator.LoadGenerator
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.LoadGenerator.LoadGenerator
			));

		private static int numMapTasks = 1;

		private string mrOutDir;

		private const string UsageCmd = "java LoadGeneratorMR\n";

		private const string Usage = UsageCmd + "-mr <numMapJobs> <outputDir> [MUST be first 3 args] \n"
			 + UsageArgs;

		private static readonly Text OpenExectime = new Text("OpenExecutionTime");

		private static readonly Text NumopsOpen = new Text("NumOpsOpen");

		private static readonly Text ListExectime = new Text("ListExecutionTime");

		private static readonly Text NumopsList = new Text("NumOpsList");

		private static readonly Text DeleteExectime = new Text("DeletionExecutionTime");

		private static readonly Text NumopsDelete = new Text("NumOpsDelete");

		private static readonly Text CreateExectime = new Text("CreateExecutionTime");

		private static readonly Text NumopsCreate = new Text("NumOpsCreate");

		private static readonly Text WriteCloseExectime = new Text("WriteCloseExecutionTime"
			);

		private static readonly Text NumopsWriteClose = new Text("NumOpsWriteClose");

		private static readonly Text ElapsedTime = new Text("ElapsedTime");

		private static readonly Text Totalops = new Text("TotalOps");

		private const string LgRoot = "LG.root";

		private const string LgScriptfile = "LG.scriptFile";

		private const string LgMaxdelaybetweenops = "LG.maxDelayBetweenOps";

		private const string LgNumofthreads = "LG.numOfThreads";

		private const string LgReadpr = "LG.readPr";

		private const string LgWritepr = "LG.writePr";

		private const string LgSeed = "LG.r";

		private const string LgNummaptasks = "LG.numMapTasks";

		private const string LgElapsedtime = "LG.elapsedTime";

		private const string LgStarttime = "LG.startTime";

		private const string LgFlagfile = "LG.flagFile";

		/// <summary>Constructor</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.UnknownHostException"/>
		public LoadGeneratorMR()
			: base()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.UnknownHostException"/>
		public LoadGeneratorMR(Configuration conf)
			: this()
		{
			// Constant "keys" used to communicate between map and reduce
			// Config keys to pass args from Main to the Job
			SetConf(conf);
		}

		/// <summary>Main function called by tool runner.</summary>
		/// <remarks>
		/// Main function called by tool runner.
		/// It first initializes data by parsing the command line arguments.
		/// It then calls the loadGenerator
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public override int Run(string[] args)
		{
			int exitCode = ParseArgsMR(args);
			if (exitCode != 0)
			{
				return exitCode;
			}
			System.Console.Out.WriteLine("Running LoadGeneratorMR against fileSystem: " + FileContext
				.GetFileContext().GetDefaultFileSystem().GetUri());
			return SubmitAsMapReduce();
		}

		// reducer will print the results
		/// <summary>Parse the command line arguments and initialize the data.</summary>
		/// <remarks>
		/// Parse the command line arguments and initialize the data.
		/// Only parse the first arg: -mr <numMapTasks> <mrOutDir> (MUST be first three Args)
		/// The rest are parsed by the Parent LoadGenerator
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private int ParseArgsMR(string[] args)
		{
			try
			{
				if (args.Length >= 3 && args[0].Equals("-mr"))
				{
					numMapTasks = System.Convert.ToInt32(args[1]);
					mrOutDir = args[2];
					if (mrOutDir.StartsWith("-"))
					{
						System.Console.Error.WriteLine("Missing output file parameter, instead got: " + mrOutDir
							);
						System.Console.Error.WriteLine(Usage);
						return -1;
					}
				}
				else
				{
					System.Console.Error.WriteLine(Usage);
					ToolRunner.PrintGenericCommandUsage(System.Console.Error);
					return -1;
				}
				string[] strippedArgs = new string[args.Length - 3];
				for (int i = 0; i < strippedArgs.Length; i++)
				{
					strippedArgs[i] = args[i + 3];
				}
				base.ParseArgs(true, strippedArgs);
			}
			catch (FormatException e)
			{
				// Parse normal LoadGenerator args
				System.Console.Error.WriteLine("Illegal parameter: " + e.GetLocalizedMessage());
				System.Console.Error.WriteLine(Usage);
				return -1;
			}
			return 0;
		}

		/// <summary>Main program</summary>
		/// <param name="args">command line arguments</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new Org.Apache.Hadoop.FS.LoadGenerator.LoadGeneratorMR
				(), args);
			System.Environment.Exit(res);
		}

		// The following methods are only used when LoadGenerator is run a MR job
		/// <summary>Based on args we submit the LoadGenerator as MR job.</summary>
		/// <remarks>
		/// Based on args we submit the LoadGenerator as MR job.
		/// Number of MapTasks is numMapTasks
		/// </remarks>
		/// <returns>exitCode for job submission</returns>
		private int SubmitAsMapReduce()
		{
			System.Console.Out.WriteLine("Running as a MapReduce job with " + numMapTasks + " mapTasks;  Output to file "
				 + mrOutDir);
			Configuration conf = new Configuration(GetConf());
			// First set all the args of LoadGenerator as Conf vars to pass to MR tasks
			conf.Set(LgRoot, root.ToString());
			conf.SetInt(LgMaxdelaybetweenops, maxDelayBetweenOps);
			conf.SetInt(LgNumofthreads, numOfThreads);
			conf.Set(LgReadpr, readProbs[0] + string.Empty);
			//Pass Double as string
			conf.Set(LgWritepr, writeProbs[0] + string.Empty);
			//Pass Double as string
			conf.SetLong(LgSeed, seed);
			//No idea what this is
			conf.SetInt(LgNummaptasks, numMapTasks);
			if (scriptFile == null && durations[0] <= 0)
			{
				System.Console.Error.WriteLine("When run as a MapReduce job, elapsed Time or ScriptFile must be specified"
					);
				System.Environment.Exit(-1);
			}
			conf.SetLong(LgElapsedtime, durations[0]);
			conf.SetLong(LgStarttime, startTime);
			if (scriptFile != null)
			{
				conf.Set(LgScriptfile, scriptFile);
			}
			conf.Set(LgFlagfile, flagFile.ToString());
			// Now set the necessary conf variables that apply to run MR itself.
			JobConf jobConf = new JobConf(conf, typeof(Org.Apache.Hadoop.FS.LoadGenerator.LoadGenerator
				));
			jobConf.SetJobName("NNLoadGeneratorViaMR");
			jobConf.SetNumMapTasks(numMapTasks);
			jobConf.SetNumReduceTasks(1);
			// 1 reducer to collect the results
			jobConf.SetOutputKeyClass(typeof(Text));
			jobConf.SetOutputValueClass(typeof(IntWritable));
			jobConf.SetMapperClass(typeof(LoadGeneratorMR.MapperThatRunsNNLoadGenerator));
			jobConf.SetReducerClass(typeof(LoadGeneratorMR.ReducerThatCollectsLGdata));
			jobConf.SetInputFormat(typeof(LoadGeneratorMR.DummyInputFormat));
			jobConf.SetOutputFormat(typeof(TextOutputFormat));
			// Explicitly set number of max map attempts to 1.
			jobConf.SetMaxMapAttempts(1);
			// Explicitly turn off speculative execution
			jobConf.SetSpeculativeExecution(false);
			// This mapReduce job has no input but has output
			FileOutputFormat.SetOutputPath(jobConf, new Path(mrOutDir));
			try
			{
				JobClient.RunJob(jobConf);
			}
			catch (IOException e)
			{
				System.Console.Error.WriteLine("Failed to run job: " + e.Message);
				return -1;
			}
			return 0;
		}

		public class EmptySplit : InputSplit
		{
			// Each split is empty
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
			}

			public virtual long GetLength()
			{
				return 0L;
			}

			public virtual string[] GetLocations()
			{
				return new string[0];
			}
		}

		public class DummyInputFormat : Configured, InputFormat<LongWritable, Text>
		{
			// Dummy Input format to send 1 record - number of spits is numMapTasks
			public virtual InputSplit[] GetSplits(JobConf conf, int numSplits)
			{
				numSplits = conf.GetInt("LG.numMapTasks", 1);
				InputSplit[] ret = new InputSplit[numSplits];
				for (int i = 0; i < numSplits; ++i)
				{
					ret[i] = new LoadGeneratorMR.EmptySplit();
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RecordReader<LongWritable, Text> GetRecordReader(InputSplit ignored
				, JobConf conf, Reporter reporter)
			{
				return new _RecordReader_267();
			}

			private sealed class _RecordReader_267 : RecordReader<LongWritable, Text>
			{
				public _RecordReader_267()
				{
					this.sentOneRecord = false;
				}

				internal bool sentOneRecord;

				/// <exception cref="System.IO.IOException"/>
				public bool Next(LongWritable key, Text value)
				{
					key.Set(1);
					value.Set("dummy");
					if (this.sentOneRecord == false)
					{
						// first call
						this.sentOneRecord = true;
						return true;
					}
					return false;
				}

				// we have sent one record - we are done
				public LongWritable CreateKey()
				{
					return new LongWritable();
				}

				public Text CreateValue()
				{
					return new Text();
				}

				/// <exception cref="System.IO.IOException"/>
				public long GetPos()
				{
					return 1;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Close()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public float GetProgress()
				{
					return 1;
				}
			}
		}

		public class MapperThatRunsNNLoadGenerator : MapReduceBase, Mapper<LongWritable, 
			Text, Text, IntWritable>
		{
			private JobConf jobConf;

			public override void Configure(JobConf job)
			{
				this.jobConf = job;
				GetArgsFromConfiguration(jobConf);
			}

			private class ProgressThread : Sharpen.Thread
			{
				internal bool keepGoing;

				private Reporter reporter;

				public ProgressThread(MapperThatRunsNNLoadGenerator _enclosing, Reporter r)
				{
					this._enclosing = _enclosing;
					// while this is true, thread runs.
					this.reporter = r;
					this.keepGoing = true;
				}

				public override void Run()
				{
					while (this.keepGoing)
					{
						if (!LoadGeneratorMR.MapperThatRunsNNLoadGenerator.ProgressThread.Interrupted())
						{
							try
							{
								Sharpen.Thread.Sleep(30 * 1000);
							}
							catch (Exception)
							{
							}
						}
						this.reporter.Progress();
					}
				}

				private readonly MapperThatRunsNNLoadGenerator _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<Text, IntWritable
				> output, Reporter reporter)
			{
				LoadGeneratorMR.MapperThatRunsNNLoadGenerator.ProgressThread progressThread = new 
					LoadGeneratorMR.MapperThatRunsNNLoadGenerator.ProgressThread(this, reporter);
				progressThread.Start();
				try
				{
					new Org.Apache.Hadoop.FS.LoadGenerator.LoadGenerator(jobConf).GenerateLoadOnNN();
					System.Console.Out.WriteLine("Finished generating load on NN, sending results to the reducer"
						);
					PrintResults(System.Console.Out);
					progressThread.keepGoing = false;
					progressThread.Join();
					// Send results to Reducer
					output.Collect(OpenExectime, new IntWritable((int)executionTime[Open]));
					output.Collect(NumopsOpen, new IntWritable((int)numOfOps[Open]));
					output.Collect(ListExectime, new IntWritable((int)executionTime[List]));
					output.Collect(NumopsList, new IntWritable((int)numOfOps[List]));
					output.Collect(DeleteExectime, new IntWritable((int)executionTime[Delete]));
					output.Collect(NumopsDelete, new IntWritable((int)numOfOps[Delete]));
					output.Collect(CreateExectime, new IntWritable((int)executionTime[Create]));
					output.Collect(NumopsCreate, new IntWritable((int)numOfOps[Create]));
					output.Collect(WriteCloseExectime, new IntWritable((int)executionTime[WriteClose]
						));
					output.Collect(NumopsWriteClose, new IntWritable((int)numOfOps[WriteClose]));
					output.Collect(Totalops, new IntWritable((int)totalOps));
					output.Collect(ElapsedTime, new IntWritable((int)totalTime));
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			public virtual void GetArgsFromConfiguration(Configuration conf)
			{
				maxDelayBetweenOps = conf.GetInt(LgMaxdelaybetweenops, maxDelayBetweenOps);
				numOfThreads = conf.GetInt(LgNumofthreads, numOfThreads);
				readProbs[0] = double.ParseDouble(conf.Get(LgReadpr, readProbs[0] + string.Empty)
					);
				writeProbs[0] = double.ParseDouble(conf.Get(LgWritepr, writeProbs[0] + string.Empty
					));
				seed = conf.GetLong(LgSeed, seed);
				numMapTasks = conf.GetInt(LgNummaptasks, numMapTasks);
				root = new Path(conf.Get(LgRoot, root.ToString()));
				durations[0] = conf.GetLong(LgElapsedtime, 0);
				startTime = conf.GetLong(LgStarttime, 0);
				scriptFile = conf.Get(LgScriptfile, null);
				flagFile = new Path(conf.Get(LgFlagfile, FlagfileDefault));
				if (durations[0] > 0 && scriptFile != null)
				{
					System.Console.Error.WriteLine("Cannot specify both ElapsedTime and ScriptFile, exiting"
						);
					System.Environment.Exit(-1);
				}
				try
				{
					if (scriptFile != null && LoadScriptFile(scriptFile, false) < 0)
					{
						System.Console.Error.WriteLine("Error in scriptFile, exiting");
						System.Environment.Exit(-1);
					}
				}
				catch (IOException e)
				{
					System.Console.Error.WriteLine("Error loading script file " + scriptFile);
					Sharpen.Runtime.PrintStackTrace(e);
				}
				if (durations[0] <= 0)
				{
					System.Console.Error.WriteLine("A duration of zero or less is not allowed when running via MapReduce."
						);
					System.Environment.Exit(-1);
				}
			}
		}

		public class ReducerThatCollectsLGdata : MapReduceBase, Reducer<Text, IntWritable
			, Text, IntWritable>
		{
			private IntWritable result = new IntWritable();

			private JobConf jobConf;

			public override void Configure(JobConf job)
			{
				this.jobConf = job;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(Text key, IEnumerator<IntWritable> values, OutputCollector
				<Text, IntWritable> output, Reporter reporter)
			{
				int sum = 0;
				while (values.HasNext())
				{
					sum += values.Next().Get();
				}
				if (key.Equals(OpenExectime))
				{
					executionTime[Open] = sum;
				}
				else
				{
					if (key.Equals(NumopsOpen))
					{
						numOfOps[Open] = sum;
					}
					else
					{
						if (key.Equals(ListExectime))
						{
							executionTime[List] = sum;
						}
						else
						{
							if (key.Equals(NumopsList))
							{
								numOfOps[List] = sum;
							}
							else
							{
								if (key.Equals(DeleteExectime))
								{
									executionTime[Delete] = sum;
								}
								else
								{
									if (key.Equals(NumopsDelete))
									{
										numOfOps[Delete] = sum;
									}
									else
									{
										if (key.Equals(CreateExectime))
										{
											executionTime[Create] = sum;
										}
										else
										{
											if (key.Equals(NumopsCreate))
											{
												numOfOps[Create] = sum;
											}
											else
											{
												if (key.Equals(WriteCloseExectime))
												{
													System.Console.Out.WriteLine(WriteCloseExectime + " = " + sum);
													executionTime[WriteClose] = sum;
												}
												else
												{
													if (key.Equals(NumopsWriteClose))
													{
														numOfOps[WriteClose] = sum;
													}
													else
													{
														if (key.Equals(Totalops))
														{
															totalOps = sum;
														}
														else
														{
															if (key.Equals(ElapsedTime))
															{
																totalTime = sum;
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
				result.Set(sum);
				output.Collect(key, result);
			}

			// System.out.println("Key = " + key + " Sum is =" + sum);
			// printResults(System.out);
			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				// Output the result to a file Results in the output dir
				FileContext fc;
				try
				{
					fc = FileContext.GetFileContext(jobConf);
				}
				catch (IOException ioe)
				{
					System.Console.Error.WriteLine("Can not initialize the file system: " + ioe.GetLocalizedMessage
						());
					return;
				}
				FSDataOutputStream o = fc.Create(FileOutputFormat.GetTaskOutputPath(jobConf, "Results"
					), EnumSet.Of(CreateFlag.Create));
				TextWriter @out = new TextWriter(o);
				PrintResults(@out);
				@out.Close();
				o.Close();
			}
		}
	}
}
