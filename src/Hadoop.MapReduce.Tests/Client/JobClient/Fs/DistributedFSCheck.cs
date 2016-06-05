using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Distributed checkup of the file system consistency.</summary>
	/// <remarks>
	/// Distributed checkup of the file system consistency.
	/// <p>
	/// Test file system consistency by reading each block of each file
	/// of the specified file tree.
	/// Report corrupted blocks and general file statistics.
	/// <p>
	/// Optionally displays statistics on read performance.
	/// </remarks>
	public class DistributedFSCheck : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.DistributedFSCheck
			));

		private const int TestTypeRead = 0;

		private const int TestTypeCleanup = 2;

		private const int DefaultBufferSize = 1000000;

		private const string DefaultResFileName = "DistributedFSCheck_results.log";

		private const long Mega = unchecked((int)(0x100000));

		private static Configuration fsConfig = new Configuration();

		private static Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", 
			"/benchmarks/DistributedFSCheck"));

		private static Path MapInputDir = new Path(TestRootDir, "map_input");

		private static Path ReadDir = new Path(TestRootDir, "io_read");

		private FileSystem fs;

		private long nrFiles;

		/// <exception cref="System.Exception"/>
		internal DistributedFSCheck(Configuration conf)
		{
			// Constants
			fsConfig = conf;
			this.fs = FileSystem.Get(conf);
		}

		/// <summary>Run distributed checkup for the entire files system.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFSBlocks()
		{
			TestFSBlocks("/");
		}

		/// <summary>Run distributed checkup for the specified directory.</summary>
		/// <param name="rootName">root directory name</param>
		/// <exception cref="System.Exception"/>
		public virtual void TestFSBlocks(string rootName)
		{
			CreateInputFile(rootName);
			RunDistributedFSCheck();
			Cleanup();
		}

		// clean up after all to restore the system state
		/// <exception cref="System.IO.IOException"/>
		private void CreateInputFile(string rootName)
		{
			Cleanup();
			// clean up if previous run failed
			Path inputFile = new Path(MapInputDir, "in_file");
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, fsConfig, inputFile, typeof(
				Text), typeof(LongWritable), SequenceFile.CompressionType.None);
			try
			{
				nrFiles = 0;
				ListSubtree(new Path(rootName), writer);
			}
			finally
			{
				writer.Close();
			}
			Log.Info("Created map input files.");
		}

		/// <exception cref="System.IO.IOException"/>
		private void ListSubtree(Path rootFile, SequenceFile.Writer writer)
		{
			FileStatus rootStatus = fs.GetFileStatus(rootFile);
			ListSubtree(rootStatus, writer);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ListSubtree(FileStatus rootStatus, SequenceFile.Writer writer)
		{
			Path rootFile = rootStatus.GetPath();
			if (rootStatus.IsFile())
			{
				nrFiles++;
				// For a regular file generate <fName,offset> pairs
				long blockSize = fs.GetDefaultBlockSize(rootFile);
				long fileLength = rootStatus.GetLen();
				for (long offset = 0; offset < fileLength; offset += blockSize)
				{
					writer.Append(new Text(rootFile.ToString()), new LongWritable(offset));
				}
				return;
			}
			FileStatus[] children = null;
			try
			{
				children = fs.ListStatus(rootFile);
			}
			catch (FileNotFoundException)
			{
				throw new IOException("Could not get listing for " + rootFile);
			}
			for (int i = 0; i < children.Length; i++)
			{
				ListSubtree(children[i], writer);
			}
		}

		/// <summary>DistributedFSCheck mapper class.</summary>
		public class DistributedFSCheckMapper : IOMapperBase<object>
		{
			public DistributedFSCheckMapper()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override object DoIO(Reporter reporter, string name, long offset)
			{
				// open file
				FSDataInputStream @in = null;
				Path p = new Path(name);
				try
				{
					@in = fs.Open(p);
				}
				catch (IOException)
				{
					return name + "@(missing)";
				}
				@in.Seek(offset);
				long actualSize = 0;
				try
				{
					long blockSize = fs.GetDefaultBlockSize(p);
					reporter.SetStatus("reading " + name + "@" + offset + "/" + blockSize);
					for (int curSize = bufferSize; curSize == bufferSize && actualSize < blockSize; actualSize
						 += curSize)
					{
						curSize = @in.Read(buffer, 0, bufferSize);
					}
				}
				catch (IOException)
				{
					Log.Info("Corrupted block detected in \"" + name + "\" at " + offset);
					return name + "@" + offset;
				}
				finally
				{
					@in.Close();
				}
				return actualSize;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void CollectStats(OutputCollector<Text, Text> output, string name
				, long execTime, object corruptedBlock)
			{
				output.Collect(new Text(AccumulatingReducer.ValueTypeLong + "blocks"), new Text(1
					.ToString()));
				if (corruptedBlock.GetType().FullName.EndsWith("String"))
				{
					output.Collect(new Text(AccumulatingReducer.ValueTypeString + "badBlocks"), new Text
						((string)corruptedBlock));
					return;
				}
				long totalSize = ((long)corruptedBlock);
				float ioRateMbSec = (float)totalSize * 1000 / (execTime * unchecked((int)(0x100000
					)));
				Log.Info("Number of bytes processed = " + totalSize);
				Log.Info("Exec time = " + execTime);
				Log.Info("IO rate = " + ioRateMbSec);
				output.Collect(new Text(AccumulatingReducer.ValueTypeLong + "size"), new Text(totalSize
					.ToString()));
				output.Collect(new Text(AccumulatingReducer.ValueTypeLong + "time"), new Text(execTime
					.ToString()));
				output.Collect(new Text(AccumulatingReducer.ValueTypeFloat + "rate"), new Text((ioRateMbSec
					 * 1000).ToString()));
			}
		}

		/// <exception cref="System.Exception"/>
		private void RunDistributedFSCheck()
		{
			JobConf job = new JobConf(fs.GetConf(), typeof(DistributedFSCheck));
			FileInputFormat.SetInputPaths(job, MapInputDir);
			job.SetInputFormat(typeof(SequenceFileInputFormat));
			job.SetMapperClass(typeof(DistributedFSCheck.DistributedFSCheckMapper));
			job.SetReducerClass(typeof(AccumulatingReducer));
			FileOutputFormat.SetOutputPath(job, ReadDir);
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetNumReduceTasks(1);
			JobClient.RunJob(job);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int testType = TestTypeRead;
			int bufferSize = DefaultBufferSize;
			string resFileName = DefaultResFileName;
			string rootName = "/";
			bool viewStats = false;
			string usage = "Usage: DistributedFSCheck [-root name] [-clean] [-resFile resultFileName] [-bufferSize Bytes] [-stats] ";
			if (args.Length == 1 && args[0].StartsWith("-h"))
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-root"))
				{
					rootName = args[++i];
				}
				else
				{
					if (args[i].StartsWith("-clean"))
					{
						testType = TestTypeCleanup;
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
							else
							{
								if (args[i].StartsWith("-stat"))
								{
									viewStats = true;
								}
							}
						}
					}
				}
			}
			Log.Info("root = " + rootName);
			Log.Info("bufferSize = " + bufferSize);
			Configuration conf = new Configuration();
			conf.SetInt("test.io.file.buffer.size", bufferSize);
			DistributedFSCheck test = new DistributedFSCheck(conf);
			if (testType == TestTypeCleanup)
			{
				test.Cleanup();
				return;
			}
			test.CreateInputFile(rootName);
			long tStart = Runtime.CurrentTimeMillis();
			test.RunDistributedFSCheck();
			long execTime = Runtime.CurrentTimeMillis() - tStart;
			test.AnalyzeResult(execTime, resFileName, viewStats);
		}

		// test.cleanup();  // clean up after all to restore the system state
		/// <exception cref="System.IO.IOException"/>
		private void AnalyzeResult(long execTime, string resFileName, bool viewStats)
		{
			Path reduceFile = new Path(ReadDir, "part-00000");
			DataInputStream @in;
			@in = new DataInputStream(fs.Open(reduceFile));
			BufferedReader lines;
			lines = new BufferedReader(new InputStreamReader(@in));
			long blocks = 0;
			long size = 0;
			long time = 0;
			float rate = 0;
			StringTokenizer badBlocks = null;
			long nrBadBlocks = 0;
			string line;
			while ((line = lines.ReadLine()) != null)
			{
				StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
				string attr = tokens.NextToken();
				if (attr.EndsWith("blocks"))
				{
					blocks = long.Parse(tokens.NextToken());
				}
				else
				{
					if (attr.EndsWith("size"))
					{
						size = long.Parse(tokens.NextToken());
					}
					else
					{
						if (attr.EndsWith("time"))
						{
							time = long.Parse(tokens.NextToken());
						}
						else
						{
							if (attr.EndsWith("rate"))
							{
								rate = float.ParseFloat(tokens.NextToken());
							}
							else
							{
								if (attr.EndsWith("badBlocks"))
								{
									badBlocks = new StringTokenizer(tokens.NextToken(), ";");
									nrBadBlocks = badBlocks.CountTokens();
								}
							}
						}
					}
				}
			}
			Vector<string> resultLines = new Vector<string>();
			resultLines.AddItem("----- DistributedFSCheck ----- : ");
			resultLines.AddItem("               Date & time: " + Sharpen.Extensions.CreateDate
				(Runtime.CurrentTimeMillis()));
			resultLines.AddItem("    Total number of blocks: " + blocks);
			resultLines.AddItem("    Total number of  files: " + nrFiles);
			resultLines.AddItem("Number of corrupted blocks: " + nrBadBlocks);
			int nrBadFilesPos = resultLines.Count;
			TreeSet<string> badFiles = new TreeSet<string>();
			long nrBadFiles = 0;
			if (nrBadBlocks > 0)
			{
				resultLines.AddItem(string.Empty);
				resultLines.AddItem("----- Corrupted Blocks (file@offset) ----- : ");
				while (badBlocks.HasMoreTokens())
				{
					string curBlock = badBlocks.NextToken();
					resultLines.AddItem(curBlock);
					badFiles.AddItem(Sharpen.Runtime.Substring(curBlock, 0, curBlock.IndexOf('@')));
				}
				nrBadFiles = badFiles.Count;
			}
			resultLines.InsertElementAt(" Number of corrupted files: " + nrBadFiles, nrBadFilesPos
				);
			if (viewStats)
			{
				resultLines.AddItem(string.Empty);
				resultLines.AddItem("-----   Performance  ----- : ");
				resultLines.AddItem("         Total MBytes read: " + size / Mega);
				resultLines.AddItem("         Throughput mb/sec: " + (float)size * 1000.0 / (time
					 * Mega));
				resultLines.AddItem("    Average IO rate mb/sec: " + rate / 1000 / blocks);
				resultLines.AddItem("        Test exec time sec: " + (float)execTime / 1000);
			}
			TextWriter res = new TextWriter(new FileOutputStream(new FilePath(resFileName), true
				));
			for (int i = 0; i < resultLines.Count; i++)
			{
				string cur = resultLines[i];
				Log.Info(cur);
				res.WriteLine(cur);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void Cleanup()
		{
			Log.Info("Cleaning up test files");
			fs.Delete(TestRootDir, true);
		}
	}
}
