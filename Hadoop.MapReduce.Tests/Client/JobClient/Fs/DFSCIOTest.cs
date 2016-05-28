using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Distributed i/o benchmark.</summary>
	/// <remarks>
	/// Distributed i/o benchmark.
	/// <p>
	/// This test writes into or reads from a specified number of files.
	/// File size is specified as a parameter to the test.
	/// Each file is accessed in a separate map task.
	/// <p>
	/// The reducer collects the following statistics:
	/// <ul>
	/// <li>number of tasks completed</li>
	/// <li>number of bytes written/read</li>
	/// <li>execution time</li>
	/// <li>io rate</li>
	/// <li>io rate squared</li>
	/// </ul>
	/// Finally, the following information is appended to a local file
	/// <ul>
	/// <li>read or write test</li>
	/// <li>date and time the test finished</li>
	/// <li>number of files</li>
	/// <li>total number of bytes processed</li>
	/// <li>throughput in mb/sec (total number of bytes / sum of processing times)</li>
	/// <li>average i/o rate in mb/sec per file</li>
	/// <li>standard i/o rate deviation</li>
	/// </ul>
	/// </remarks>
	public class DFSCIOTest : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DFSCIOTest));

		private const int TestTypeRead = 0;

		private const int TestTypeWrite = 1;

		private const int TestTypeCleanup = 2;

		private const int DefaultBufferSize = 1000000;

		private const string BaseFileName = "test_io_";

		private const string DefaultResFileName = "DFSCIOTest_results.log";

		private static Configuration fsConfig = new Configuration();

		private const long Mega = unchecked((int)(0x100000));

		private static string TestRootDir = Runtime.GetProperty("test.build.data", "/benchmarks/DFSCIOTest"
			);

		private static Path ControlDir = new Path(TestRootDir, "io_control");

		private static Path WriteDir = new Path(TestRootDir, "io_write");

		private static Path ReadDir = new Path(TestRootDir, "io_read");

		private static Path DataDir = new Path(TestRootDir, "io_data");

		private static Path HdfsTestDir = new Path("/tmp/DFSCIOTest");

		private static string HdfsLibVersion = Runtime.GetProperty("libhdfs.version", "1"
			);

		private static string Chmod = new string("chmod");

		private static Path HdfsShlib = new Path(HdfsTestDir + "/libhdfs.so." + HdfsLibVersion
			);

		private static Path HdfsRead = new Path(HdfsTestDir + "/hdfs_read");

		private static Path HdfsWrite = new Path(HdfsTestDir + "/hdfs_write");

		// Constants
		/// <summary>Run the test with default parameters.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestIOs()
		{
			TestIOs(10, 10);
		}

		/// <summary>Run the test with the specified parameters.</summary>
		/// <param name="fileSize">file size</param>
		/// <param name="nrFiles">number of files</param>
		/// <exception cref="System.IO.IOException"/>
		public static void TestIOs(int fileSize, int nrFiles)
		{
			FileSystem fs = FileSystem.Get(fsConfig);
			CreateControlFile(fs, fileSize, nrFiles);
			WriteTest(fs);
			ReadTest(fs);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateControlFile(FileSystem fs, int fileSize, int nrFiles)
		{
			// in MB 
			Log.Info("creating control file: " + fileSize + " mega bytes, " + nrFiles + " files"
				);
			fs.Delete(ControlDir, true);
			for (int i = 0; i < nrFiles; i++)
			{
				string name = GetFileName(i);
				Path controlFile = new Path(ControlDir, "in_file_" + name);
				SequenceFile.Writer writer = null;
				try
				{
					writer = SequenceFile.CreateWriter(fs, fsConfig, controlFile, typeof(Text), typeof(
						LongWritable), SequenceFile.CompressionType.None);
					writer.Append(new Text(name), new LongWritable(fileSize));
				}
				catch (Exception e)
				{
					throw new IOException(e.GetLocalizedMessage());
				}
				finally
				{
					if (writer != null)
					{
						writer.Close();
					}
					writer = null;
				}
			}
			Log.Info("created control files for: " + nrFiles + " files");
		}

		private static string GetFileName(int fIdx)
		{
			return BaseFileName + Sharpen.Extensions.ToString(fIdx);
		}

		/// <summary>Write/Read mapper base class.</summary>
		/// <remarks>
		/// Write/Read mapper base class.
		/// <p>
		/// Collects the following statistics per task:
		/// <ul>
		/// <li>number of tasks completed</li>
		/// <li>number of bytes written/read</li>
		/// <li>execution time</li>
		/// <li>i/o rate</li>
		/// <li>i/o rate squared</li>
		/// </ul>
		/// </remarks>
		private abstract class IOStatMapper : IOMapperBase<long>
		{
			internal IOStatMapper()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void CollectStats(OutputCollector<Text, Text> output, string name
				, long execTime, long objSize)
			{
				long totalSize = objSize;
				float ioRateMbSec = (float)totalSize * 1000 / (execTime * Mega);
				Log.Info("Number of bytes processed = " + totalSize);
				Log.Info("Exec time = " + execTime);
				Log.Info("IO rate = " + ioRateMbSec);
				output.Collect(new Text(AccumulatingReducer.ValueTypeLong + "tasks"), new Text(1.
					ToString()));
				output.Collect(new Text(AccumulatingReducer.ValueTypeLong + "size"), new Text(totalSize
					.ToString()));
				output.Collect(new Text(AccumulatingReducer.ValueTypeLong + "time"), new Text(execTime
					.ToString()));
				output.Collect(new Text(AccumulatingReducer.ValueTypeFloat + "rate"), new Text((ioRateMbSec
					 * 1000).ToString()));
				output.Collect(new Text(AccumulatingReducer.ValueTypeFloat + "sqrate"), new Text(
					(ioRateMbSec * ioRateMbSec * 1000).ToString()));
			}
		}

		/// <summary>Write mapper class.</summary>
		public class WriteMapper : DFSCIOTest.IOStatMapper
		{
			public WriteMapper()
				: base()
			{
				for (int i = 0; i < bufferSize; i++)
				{
					buffer[i] = unchecked((byte)((byte)('0') + i % 50));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long DoIO(Reporter reporter, string name, long totalSize)
			{
				// create file
				totalSize *= Mega;
				// create instance of local filesystem 
				FileSystem localFS = FileSystem.GetLocal(fsConfig);
				try
				{
					// native runtime
					Runtime runTime = Runtime.GetRuntime();
					// copy the dso and executable from dfs and chmod them
					lock (this)
					{
						localFS.Delete(HdfsTestDir, true);
						if (!(localFS.Mkdirs(HdfsTestDir)))
						{
							throw new IOException("Failed to create " + HdfsTestDir + " on local filesystem");
						}
					}
					lock (this)
					{
						if (!localFS.Exists(HdfsShlib))
						{
							FileUtil.Copy(fs, HdfsShlib, localFS, HdfsShlib, false, fsConfig);
							string chmodCmd = new string(Chmod + " a+x " + HdfsShlib);
							SystemProcess process = runTime.Exec(chmodCmd);
							int exitStatus = process.WaitFor();
							if (exitStatus != 0)
							{
								throw new IOException(chmodCmd + ": Failed with exitStatus: " + exitStatus);
							}
						}
					}
					lock (this)
					{
						if (!localFS.Exists(HdfsWrite))
						{
							FileUtil.Copy(fs, HdfsWrite, localFS, HdfsWrite, false, fsConfig);
							string chmodCmd = new string(Chmod + " a+x " + HdfsWrite);
							SystemProcess process = runTime.Exec(chmodCmd);
							int exitStatus = process.WaitFor();
							if (exitStatus != 0)
							{
								throw new IOException(chmodCmd + ": Failed with exitStatus: " + exitStatus);
							}
						}
					}
					// exec the C program
					Path outFile = new Path(DataDir, name);
					string writeCmd = new string(HdfsWrite + " " + outFile + " " + totalSize + " " + 
						bufferSize);
					SystemProcess process_1 = runTime.Exec(writeCmd, null, new FilePath(HdfsTestDir.ToString
						()));
					int exitStatus_1 = process_1.WaitFor();
					if (exitStatus_1 != 0)
					{
						throw new IOException(writeCmd + ": Failed with exitStatus: " + exitStatus_1);
					}
				}
				catch (Exception interruptedException)
				{
					reporter.SetStatus(interruptedException.ToString());
				}
				finally
				{
					localFS.Close();
				}
				return totalSize;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteTest(FileSystem fs)
		{
			fs.Delete(DataDir, true);
			fs.Delete(WriteDir, true);
			RunIOTest(typeof(DFSCIOTest.WriteMapper), WriteDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunIOTest(Type mapperClass, Path outputDir)
		{
			JobConf job = new JobConf(fsConfig, typeof(DFSCIOTest));
			FileInputFormat.SetInputPaths(job, ControlDir);
			job.SetInputFormat(typeof(SequenceFileInputFormat));
			job.SetMapperClass(mapperClass);
			job.SetReducerClass(typeof(AccumulatingReducer));
			FileOutputFormat.SetOutputPath(job, outputDir);
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetNumReduceTasks(1);
			JobClient.RunJob(job);
		}

		/// <summary>Read mapper class.</summary>
		public class ReadMapper : DFSCIOTest.IOStatMapper
		{
			public ReadMapper()
				: base()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long DoIO(Reporter reporter, string name, long totalSize)
			{
				totalSize *= Mega;
				// create instance of local filesystem 
				FileSystem localFS = FileSystem.GetLocal(fsConfig);
				try
				{
					// native runtime
					Runtime runTime = Runtime.GetRuntime();
					// copy the dso and executable from dfs
					lock (this)
					{
						localFS.Delete(HdfsTestDir, true);
						if (!(localFS.Mkdirs(HdfsTestDir)))
						{
							throw new IOException("Failed to create " + HdfsTestDir + " on local filesystem");
						}
					}
					lock (this)
					{
						if (!localFS.Exists(HdfsShlib))
						{
							if (!FileUtil.Copy(fs, HdfsShlib, localFS, HdfsShlib, false, fsConfig))
							{
								throw new IOException("Failed to copy " + HdfsShlib + " to local filesystem");
							}
							string chmodCmd = new string(Chmod + " a+x " + HdfsShlib);
							SystemProcess process = runTime.Exec(chmodCmd);
							int exitStatus = process.WaitFor();
							if (exitStatus != 0)
							{
								throw new IOException(chmodCmd + ": Failed with exitStatus: " + exitStatus);
							}
						}
					}
					lock (this)
					{
						if (!localFS.Exists(HdfsRead))
						{
							if (!FileUtil.Copy(fs, HdfsRead, localFS, HdfsRead, false, fsConfig))
							{
								throw new IOException("Failed to copy " + HdfsRead + " to local filesystem");
							}
							string chmodCmd = new string(Chmod + " a+x " + HdfsRead);
							SystemProcess process = runTime.Exec(chmodCmd);
							int exitStatus = process.WaitFor();
							if (exitStatus != 0)
							{
								throw new IOException(chmodCmd + ": Failed with exitStatus: " + exitStatus);
							}
						}
					}
					// exec the C program
					Path inFile = new Path(DataDir, name);
					string readCmd = new string(HdfsRead + " " + inFile + " " + totalSize + " " + bufferSize
						);
					SystemProcess process_1 = runTime.Exec(readCmd, null, new FilePath(HdfsTestDir.ToString
						()));
					int exitStatus_1 = process_1.WaitFor();
					if (exitStatus_1 != 0)
					{
						throw new IOException(HdfsRead + ": Failed with exitStatus: " + exitStatus_1);
					}
				}
				catch (Exception interruptedException)
				{
					reporter.SetStatus(interruptedException.ToString());
				}
				finally
				{
					localFS.Close();
				}
				return totalSize;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ReadTest(FileSystem fs)
		{
			fs.Delete(ReadDir, true);
			RunIOTest(typeof(DFSCIOTest.ReadMapper), ReadDir);
		}

		/// <exception cref="System.Exception"/>
		private static void SequentialTest(FileSystem fs, int testType, int fileSize, int
			 nrFiles)
		{
			DFSCIOTest.IOStatMapper ioer = null;
			if (testType == TestTypeRead)
			{
				ioer = new DFSCIOTest.ReadMapper();
			}
			else
			{
				if (testType == TestTypeWrite)
				{
					ioer = new DFSCIOTest.WriteMapper();
				}
				else
				{
					return;
				}
			}
			for (int i = 0; i < nrFiles; i++)
			{
				ioer.DoIO(Reporter.Null, BaseFileName + Sharpen.Extensions.ToString(i), Mega * fileSize
					);
			}
		}

		public static void Main(string[] args)
		{
			int testType = TestTypeRead;
			int bufferSize = DefaultBufferSize;
			int fileSize = 1;
			int nrFiles = 1;
			string resFileName = DefaultResFileName;
			bool isSequential = false;
			string version = "DFSCIOTest.0.0.1";
			string usage = "Usage: DFSCIOTest -read | -write | -clean [-nrFiles N] [-fileSize MB] [-resFile resultFileName] [-bufferSize Bytes] ";
			System.Console.Out.WriteLine(version);
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].StartsWith("-r"))
				{
					testType = TestTypeRead;
				}
				else
				{
					if (args[i].StartsWith("-w"))
					{
						testType = TestTypeWrite;
					}
					else
					{
						if (args[i].StartsWith("-clean"))
						{
							testType = TestTypeCleanup;
						}
						else
						{
							if (args[i].StartsWith("-seq"))
							{
								isSequential = true;
							}
							else
							{
								if (args[i].Equals("-nrFiles"))
								{
									nrFiles = System.Convert.ToInt32(args[++i]);
								}
								else
								{
									if (args[i].Equals("-fileSize"))
									{
										fileSize = System.Convert.ToInt32(args[++i]);
									}
									else
									{
										if (args[i].Equals("-bufferSize"))
										{
											bufferSize = System.Convert.ToInt32(args[++i]);
										}
										else
										{
											if (args[i].Equals("-resFile"))
											{
												resFileName = args[++i];
											}
										}
									}
								}
							}
						}
					}
				}
			}
			Log.Info("nrFiles = " + nrFiles);
			Log.Info("fileSize (MB) = " + fileSize);
			Log.Info("bufferSize = " + bufferSize);
			try
			{
				fsConfig.SetInt("test.io.file.buffer.size", bufferSize);
				FileSystem fs = FileSystem.Get(fsConfig);
				if (testType != TestTypeCleanup)
				{
					fs.Delete(HdfsTestDir, true);
					if (!fs.Mkdirs(HdfsTestDir))
					{
						throw new IOException("Mkdirs failed to create " + HdfsTestDir.ToString());
					}
					//Copy the executables over to the remote filesystem
					string hadoopHome = Runtime.Getenv("HADOOP_PREFIX");
					fs.CopyFromLocalFile(new Path(hadoopHome + "/libhdfs/libhdfs.so." + HdfsLibVersion
						), HdfsShlib);
					fs.CopyFromLocalFile(new Path(hadoopHome + "/libhdfs/hdfs_read"), HdfsRead);
					fs.CopyFromLocalFile(new Path(hadoopHome + "/libhdfs/hdfs_write"), HdfsWrite);
				}
				if (isSequential)
				{
					long tStart = Runtime.CurrentTimeMillis();
					SequentialTest(fs, testType, fileSize, nrFiles);
					long execTime = Runtime.CurrentTimeMillis() - tStart;
					string resultLine = "Seq Test exec time sec: " + (float)execTime / 1000;
					Log.Info(resultLine);
					return;
				}
				if (testType == TestTypeCleanup)
				{
					Cleanup(fs);
					return;
				}
				CreateControlFile(fs, fileSize, nrFiles);
				long tStart_1 = Runtime.CurrentTimeMillis();
				if (testType == TestTypeWrite)
				{
					WriteTest(fs);
				}
				if (testType == TestTypeRead)
				{
					ReadTest(fs);
				}
				long execTime_1 = Runtime.CurrentTimeMillis() - tStart_1;
				AnalyzeResult(fs, testType, execTime_1, resFileName);
			}
			catch (Exception e)
			{
				System.Console.Error.Write(e.GetLocalizedMessage());
				System.Environment.Exit(-1);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void AnalyzeResult(FileSystem fs, int testType, long execTime, string
			 resFileName)
		{
			Path reduceFile;
			if (testType == TestTypeWrite)
			{
				reduceFile = new Path(WriteDir, "part-00000");
			}
			else
			{
				reduceFile = new Path(ReadDir, "part-00000");
			}
			DataInputStream @in;
			@in = new DataInputStream(fs.Open(reduceFile));
			BufferedReader lines;
			lines = new BufferedReader(new InputStreamReader(@in));
			long tasks = 0;
			long size = 0;
			long time = 0;
			float rate = 0;
			float sqrate = 0;
			string line;
			while ((line = lines.ReadLine()) != null)
			{
				StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
				string attr = tokens.NextToken();
				if (attr.EndsWith(":tasks"))
				{
					tasks = long.Parse(tokens.NextToken());
				}
				else
				{
					if (attr.EndsWith(":size"))
					{
						size = long.Parse(tokens.NextToken());
					}
					else
					{
						if (attr.EndsWith(":time"))
						{
							time = long.Parse(tokens.NextToken());
						}
						else
						{
							if (attr.EndsWith(":rate"))
							{
								rate = float.ParseFloat(tokens.NextToken());
							}
							else
							{
								if (attr.EndsWith(":sqrate"))
								{
									sqrate = float.ParseFloat(tokens.NextToken());
								}
							}
						}
					}
				}
			}
			double med = rate / 1000 / tasks;
			double stdDev = Math.Sqrt(Math.Abs(sqrate / 1000 / tasks - med * med));
			string[] resultLines = new string[] { "----- DFSCIOTest ----- : " + ((testType ==
				 TestTypeWrite) ? "write" : (testType == TestTypeRead) ? "read" : "unknown"), "           Date & time: "
				 + Sharpen.Extensions.CreateDate(Runtime.CurrentTimeMillis()), "       Number of files: "
				 + tasks, "Total MBytes processed: " + size / Mega, "     Throughput mb/sec: " +
				 size * 1000.0 / (time * Mega), "Average IO rate mb/sec: " + med, " Std IO rate deviation: "
				 + stdDev, "    Test exec time sec: " + (float)execTime / 1000, string.Empty };
			TextWriter res = new TextWriter(new FileOutputStream(new FilePath(resFileName), true
				));
			for (int i = 0; i < resultLines.Length; i++)
			{
				Log.Info(resultLines[i]);
				res.WriteLine(resultLines[i]);
			}
		}

		/// <exception cref="System.Exception"/>
		private static void Cleanup(FileSystem fs)
		{
			Log.Info("Cleaning up test files");
			fs.Delete(new Path(TestRootDir), true);
			fs.Delete(HdfsTestDir, true);
		}
	}
}
