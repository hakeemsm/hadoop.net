using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Distributed i/o benchmark.</summary>
	/// <remarks>
	/// Distributed i/o benchmark.
	/// <p>
	/// This test writes into or reads from a specified number of files.
	/// Number of bytes to write or read is specified as a parameter to the test.
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
	/// <li>standard deviation of i/o rate </li>
	/// </ul>
	/// </remarks>
	public class TestDFSIO : Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.TestDFSIO
			));

		private const int DefaultBufferSize = 1000000;

		private const string BaseFileName = "test_io_";

		private const string DefaultResFileName = "TestDFSIO_results.log";

		private static readonly long Mega = TestDFSIO.ByteMultiple.Mb.Value();

		private const int DefaultNrBytes = 128;

		private const int DefaultNrFiles = 4;

		private static readonly string Usage = "Usage: " + typeof(Org.Apache.Hadoop.FS.TestDFSIO
			).Name + " [genericOptions]" + " -read [-random | -backward | -skip [-skipSize Size]] |"
			 + " -write | -append | -truncate | -clean" + " [-compression codecClassName]" +
			 " [-nrFiles N]" + " [-size Size[B|KB|MB|GB|TB]]" + " [-resFile resultFileName] [-bufferSize Bytes]"
			 + " [-rootDir]";

		private Configuration config;

		static TestDFSIO()
		{
			// Constants
			Configuration.AddDefaultResource("hdfs-default.xml");
			Configuration.AddDefaultResource("hdfs-site.xml");
			Configuration.AddDefaultResource("mapred-default.xml");
			Configuration.AddDefaultResource("mapred-site.xml");
		}

		[System.Serializable]
		private sealed class TestType
		{
			public static readonly TestDFSIO.TestType TestTypeRead = new TestDFSIO.TestType("read"
				);

			public static readonly TestDFSIO.TestType TestTypeWrite = new TestDFSIO.TestType(
				"write");

			public static readonly TestDFSIO.TestType TestTypeCleanup = new TestDFSIO.TestType
				("cleanup");

			public static readonly TestDFSIO.TestType TestTypeAppend = new TestDFSIO.TestType
				("append");

			public static readonly TestDFSIO.TestType TestTypeReadRandom = new TestDFSIO.TestType
				("random read");

			public static readonly TestDFSIO.TestType TestTypeReadBackward = new TestDFSIO.TestType
				("backward read");

			public static readonly TestDFSIO.TestType TestTypeReadSkip = new TestDFSIO.TestType
				("skip read");

			public static readonly TestDFSIO.TestType TestTypeTruncate = new TestDFSIO.TestType
				("truncate");

			private string type;

			private TestType(string t)
			{
				TestDFSIO.TestType.type = t;
			}

			public override string ToString()
			{
				// String
				return TestDFSIO.TestType.type;
			}
		}

		[System.Serializable]
		internal sealed class ByteMultiple
		{
			public static readonly TestDFSIO.ByteMultiple B = new TestDFSIO.ByteMultiple(1L);

			public static readonly TestDFSIO.ByteMultiple Kb = new TestDFSIO.ByteMultiple(unchecked(
				(long)(0x400L)));

			public static readonly TestDFSIO.ByteMultiple Mb = new TestDFSIO.ByteMultiple(unchecked(
				(long)(0x100000L)));

			public static readonly TestDFSIO.ByteMultiple Gb = new TestDFSIO.ByteMultiple(unchecked(
				(long)(0x40000000L)));

			public static readonly TestDFSIO.ByteMultiple Tb = new TestDFSIO.ByteMultiple(unchecked(
				(long)(0x10000000000L)));

			private long multiplier;

			private ByteMultiple(long mult)
			{
				TestDFSIO.ByteMultiple.multiplier = mult;
			}

			internal long Value()
			{
				return TestDFSIO.ByteMultiple.multiplier;
			}

			internal static TestDFSIO.ByteMultiple ParseString(string sMultiple)
			{
				if (sMultiple == null || sMultiple.IsEmpty())
				{
					// MB by default
					return TestDFSIO.ByteMultiple.Mb;
				}
				string sMU = StringUtils.ToUpperCase(sMultiple);
				if (StringUtils.ToUpperCase(TestDFSIO.ByteMultiple.B.ToString()).EndsWith(sMU))
				{
					return TestDFSIO.ByteMultiple.B;
				}
				if (StringUtils.ToUpperCase(TestDFSIO.ByteMultiple.Kb.ToString()).EndsWith(sMU))
				{
					return TestDFSIO.ByteMultiple.Kb;
				}
				if (StringUtils.ToUpperCase(TestDFSIO.ByteMultiple.Mb.ToString()).EndsWith(sMU))
				{
					return TestDFSIO.ByteMultiple.Mb;
				}
				if (StringUtils.ToUpperCase(TestDFSIO.ByteMultiple.Gb.ToString()).EndsWith(sMU))
				{
					return TestDFSIO.ByteMultiple.Gb;
				}
				if (StringUtils.ToUpperCase(TestDFSIO.ByteMultiple.Tb.ToString()).EndsWith(sMU))
				{
					return TestDFSIO.ByteMultiple.Tb;
				}
				throw new ArgumentException("Unsupported ByteMultiple " + sMultiple);
			}
		}

		public TestDFSIO()
		{
			this.config = new Configuration();
		}

		private static string GetBaseDir(Configuration conf)
		{
			return conf.Get("test.build.data", "/benchmarks/TestDFSIO");
		}

		private static Path GetControlDir(Configuration conf)
		{
			return new Path(GetBaseDir(conf), "io_control");
		}

		private static Path GetWriteDir(Configuration conf)
		{
			return new Path(GetBaseDir(conf), "io_write");
		}

		private static Path GetReadDir(Configuration conf)
		{
			return new Path(GetBaseDir(conf), "io_read");
		}

		private static Path GetAppendDir(Configuration conf)
		{
			return new Path(GetBaseDir(conf), "io_append");
		}

		private static Path GetRandomReadDir(Configuration conf)
		{
			return new Path(GetBaseDir(conf), "io_random_read");
		}

		private static Path GetTruncateDir(Configuration conf)
		{
			return new Path(GetBaseDir(conf), "io_truncate");
		}

		private static Path GetDataDir(Configuration conf)
		{
			return new Path(GetBaseDir(conf), "io_data");
		}

		private static MiniDFSCluster cluster;

		private static TestDFSIO bench;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void BeforeClass()
		{
			bench = new TestDFSIO();
			bench.GetConf().SetBoolean(DFSConfigKeys.DfsSupportAppendKey, true);
			bench.GetConf().SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			cluster = new MiniDFSCluster.Builder(bench.GetConf()).NumDataNodes(2).Format(true
				).Build();
			FileSystem fs = cluster.GetFileSystem();
			bench.CreateControlFile(fs, DefaultNrBytes, DefaultNrFiles);
			TestWrite();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void AfterClass()
		{
			if (cluster == null)
			{
				return;
			}
			FileSystem fs = cluster.GetFileSystem();
			bench.Cleanup(fs);
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		public static void TestWrite()
		{
			FileSystem fs = cluster.GetFileSystem();
			long tStart = Runtime.CurrentTimeMillis();
			bench.WriteTest(fs);
			long execTime = Runtime.CurrentTimeMillis() - tStart;
			bench.AnalyzeResult(fs, TestDFSIO.TestType.TestTypeWrite, execTime);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRead()
		{
			FileSystem fs = cluster.GetFileSystem();
			long tStart = Runtime.CurrentTimeMillis();
			bench.ReadTest(fs);
			long execTime = Runtime.CurrentTimeMillis() - tStart;
			bench.AnalyzeResult(fs, TestDFSIO.TestType.TestTypeRead, execTime);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadRandom()
		{
			FileSystem fs = cluster.GetFileSystem();
			long tStart = Runtime.CurrentTimeMillis();
			bench.GetConf().SetLong("test.io.skip.size", 0);
			bench.RandomReadTest(fs);
			long execTime = Runtime.CurrentTimeMillis() - tStart;
			bench.AnalyzeResult(fs, TestDFSIO.TestType.TestTypeReadRandom, execTime);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadBackward()
		{
			FileSystem fs = cluster.GetFileSystem();
			long tStart = Runtime.CurrentTimeMillis();
			bench.GetConf().SetLong("test.io.skip.size", -DefaultBufferSize);
			bench.RandomReadTest(fs);
			long execTime = Runtime.CurrentTimeMillis() - tStart;
			bench.AnalyzeResult(fs, TestDFSIO.TestType.TestTypeReadBackward, execTime);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadSkip()
		{
			FileSystem fs = cluster.GetFileSystem();
			long tStart = Runtime.CurrentTimeMillis();
			bench.GetConf().SetLong("test.io.skip.size", 1);
			bench.RandomReadTest(fs);
			long execTime = Runtime.CurrentTimeMillis() - tStart;
			bench.AnalyzeResult(fs, TestDFSIO.TestType.TestTypeReadSkip, execTime);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppend()
		{
			FileSystem fs = cluster.GetFileSystem();
			long tStart = Runtime.CurrentTimeMillis();
			bench.AppendTest(fs);
			long execTime = Runtime.CurrentTimeMillis() - tStart;
			bench.AnalyzeResult(fs, TestDFSIO.TestType.TestTypeAppend, execTime);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTruncate()
		{
			FileSystem fs = cluster.GetFileSystem();
			bench.CreateControlFile(fs, DefaultNrBytes / 2, DefaultNrFiles);
			long tStart = Runtime.CurrentTimeMillis();
			bench.TruncateTest(fs);
			long execTime = Runtime.CurrentTimeMillis() - tStart;
			bench.AnalyzeResult(fs, TestDFSIO.TestType.TestTypeTruncate, execTime);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateControlFile(FileSystem fs, long nrBytes, int nrFiles)
		{
			// in bytes
			Log.Info("creating control file: " + nrBytes + " bytes, " + nrFiles + " files");
			Path controlDir = GetControlDir(config);
			fs.Delete(controlDir, true);
			for (int i = 0; i < nrFiles; i++)
			{
				string name = GetFileName(i);
				Path controlFile = new Path(controlDir, "in_file_" + name);
				SequenceFile.Writer writer = null;
				try
				{
					writer = SequenceFile.CreateWriter(fs, config, controlFile, typeof(Text), typeof(
						LongWritable), SequenceFile.CompressionType.None);
					writer.Append(new Text(name), new LongWritable(nrBytes));
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
			protected internal CompressionCodec compressionCodec;

			internal IOStatMapper()
			{
			}

			public override void Configure(JobConf conf)
			{
				// Mapper
				base.Configure(conf);
				// grab compression
				string compression = GetConf().Get("test.io.compression.class", null);
				Type codec;
				// try to initialize codec
				try
				{
					codec = (compression == null) ? null : Sharpen.Runtime.GetType(compression).AsSubclass
						<CompressionCodec>();
				}
				catch (Exception e)
				{
					throw new RuntimeException("Compression codec not found: ", e);
				}
				if (codec != null)
				{
					compressionCodec = (CompressionCodec)ReflectionUtils.NewInstance(codec, GetConf()
						);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void CollectStats(OutputCollector<Text, Text> output, string name
				, long execTime, long objSize)
			{
				// IOMapperBase
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
		public class WriteMapper : TestDFSIO.IOStatMapper
		{
			public WriteMapper()
			{
				for (int i = 0; i < bufferSize; i++)
				{
					buffer[i] = unchecked((byte)((byte)('0') + i % 50));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDisposable GetIOStream(string name)
			{
				// IOMapperBase
				// create file
				OutputStream @out = fs.Create(new Path(GetDataDir(GetConf()), name), true, bufferSize
					);
				if (compressionCodec != null)
				{
					@out = compressionCodec.CreateOutputStream(@out);
				}
				Log.Info("out = " + @out.GetType().FullName);
				return @out;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long DoIO(Reporter reporter, string name, long totalSize)
			{
				// IOMapperBase
				// in bytes
				OutputStream @out = (OutputStream)this.stream;
				// write to the file
				long nrRemaining;
				for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize)
				{
					int curSize = (bufferSize < nrRemaining) ? bufferSize : (int)nrRemaining;
					@out.Write(buffer, 0, curSize);
					reporter.SetStatus("writing " + name + "@" + (totalSize - nrRemaining) + "/" + totalSize
						 + " ::host = " + hostName);
				}
				return Sharpen.Extensions.ValueOf(totalSize);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteTest(FileSystem fs)
		{
			Path writeDir = GetWriteDir(config);
			fs.Delete(GetDataDir(config), true);
			fs.Delete(writeDir, true);
			RunIOTest(typeof(TestDFSIO.WriteMapper), writeDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunIOTest(Type mapperClass, Path outputDir)
		{
			JobConf job = new JobConf(config, typeof(TestDFSIO));
			FileInputFormat.SetInputPaths(job, GetControlDir(config));
			job.SetInputFormat(typeof(SequenceFileInputFormat));
			job.SetMapperClass(mapperClass);
			job.SetReducerClass(typeof(AccumulatingReducer));
			FileOutputFormat.SetOutputPath(job, outputDir);
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetNumReduceTasks(1);
			JobClient.RunJob(job);
		}

		/// <summary>Append mapper class.</summary>
		public class AppendMapper : TestDFSIO.IOStatMapper
		{
			public AppendMapper()
			{
				for (int i = 0; i < bufferSize; i++)
				{
					buffer[i] = unchecked((byte)((byte)('0') + i % 50));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDisposable GetIOStream(string name)
			{
				// IOMapperBase
				// open file for append
				OutputStream @out = fs.Append(new Path(GetDataDir(GetConf()), name), bufferSize);
				if (compressionCodec != null)
				{
					@out = compressionCodec.CreateOutputStream(@out);
				}
				Log.Info("out = " + @out.GetType().FullName);
				return @out;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long DoIO(Reporter reporter, string name, long totalSize)
			{
				// IOMapperBase
				// in bytes
				OutputStream @out = (OutputStream)this.stream;
				// write to the file
				long nrRemaining;
				for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize)
				{
					int curSize = (bufferSize < nrRemaining) ? bufferSize : (int)nrRemaining;
					@out.Write(buffer, 0, curSize);
					reporter.SetStatus("writing " + name + "@" + (totalSize - nrRemaining) + "/" + totalSize
						 + " ::host = " + hostName);
				}
				return Sharpen.Extensions.ValueOf(totalSize);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void AppendTest(FileSystem fs)
		{
			Path appendDir = GetAppendDir(config);
			fs.Delete(appendDir, true);
			RunIOTest(typeof(TestDFSIO.AppendMapper), appendDir);
		}

		/// <summary>Read mapper class.</summary>
		public class ReadMapper : TestDFSIO.IOStatMapper
		{
			public ReadMapper()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDisposable GetIOStream(string name)
			{
				// IOMapperBase
				// open file
				InputStream @in = fs.Open(new Path(GetDataDir(GetConf()), name));
				if (compressionCodec != null)
				{
					@in = compressionCodec.CreateInputStream(@in);
				}
				Log.Info("in = " + @in.GetType().FullName);
				return @in;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long DoIO(Reporter reporter, string name, long totalSize)
			{
				// IOMapperBase
				// in bytes
				InputStream @in = (InputStream)this.stream;
				long actualSize = 0;
				while (actualSize < totalSize)
				{
					int curSize = @in.Read(buffer, 0, bufferSize);
					if (curSize < 0)
					{
						break;
					}
					actualSize += curSize;
					reporter.SetStatus("reading " + name + "@" + actualSize + "/" + totalSize + " ::host = "
						 + hostName);
				}
				return Sharpen.Extensions.ValueOf(actualSize);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadTest(FileSystem fs)
		{
			Path readDir = GetReadDir(config);
			fs.Delete(readDir, true);
			RunIOTest(typeof(TestDFSIO.ReadMapper), readDir);
		}

		/// <summary>Mapper class for random reads.</summary>
		/// <remarks>
		/// Mapper class for random reads.
		/// The mapper chooses a position in the file and reads bufferSize
		/// bytes starting at the chosen position.
		/// It stops after reading the totalSize bytes, specified by -size.
		/// There are three type of reads.
		/// 1) Random read always chooses a random position to read from: skipSize = 0
		/// 2) Backward read reads file in reverse order                : skipSize &lt; 0
		/// 3) Skip-read skips skipSize bytes after every read          : skipSize &gt; 0
		/// </remarks>
		public class RandomReadMapper : TestDFSIO.IOStatMapper
		{
			private Random rnd;

			private long fileSize;

			private long skipSize;

			public override void Configure(JobConf conf)
			{
				// Mapper
				base.Configure(conf);
				skipSize = conf.GetLong("test.io.skip.size", 0);
			}

			public RandomReadMapper()
			{
				rnd = new Random();
			}

			/// <exception cref="System.IO.IOException"/>
			public override IDisposable GetIOStream(string name)
			{
				// IOMapperBase
				Path filePath = new Path(GetDataDir(GetConf()), name);
				this.fileSize = fs.GetFileStatus(filePath).GetLen();
				InputStream @in = fs.Open(filePath);
				if (compressionCodec != null)
				{
					@in = new FSDataInputStream(compressionCodec.CreateInputStream(@in));
				}
				Log.Info("in = " + @in.GetType().FullName);
				Log.Info("skipSize = " + skipSize);
				return @in;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long DoIO(Reporter reporter, string name, long totalSize)
			{
				// IOMapperBase
				// in bytes
				PositionedReadable @in = (PositionedReadable)this.stream;
				long actualSize = 0;
				for (long pos = NextOffset(-1); actualSize < totalSize; pos = NextOffset(pos))
				{
					int curSize = @in.Read(pos, buffer, 0, bufferSize);
					if (curSize < 0)
					{
						break;
					}
					actualSize += curSize;
					reporter.SetStatus("reading " + name + "@" + actualSize + "/" + totalSize + " ::host = "
						 + hostName);
				}
				return Sharpen.Extensions.ValueOf(actualSize);
			}

			/// <summary>Get next offset for reading.</summary>
			/// <remarks>
			/// Get next offset for reading.
			/// If current &lt; 0 then choose initial offset according to the read type.
			/// </remarks>
			/// <param name="current">offset</param>
			/// <returns/>
			private long NextOffset(long current)
			{
				if (skipSize == 0)
				{
					return rnd.Next((int)(fileSize));
				}
				if (skipSize > 0)
				{
					return (current < 0) ? 0 : (current + bufferSize + skipSize);
				}
				// skipSize < 0
				return (current < 0) ? Math.Max(0, fileSize - bufferSize) : Math.Max(0, current +
					 skipSize);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void RandomReadTest(FileSystem fs)
		{
			Path readDir = GetRandomReadDir(config);
			fs.Delete(readDir, true);
			RunIOTest(typeof(TestDFSIO.RandomReadMapper), readDir);
		}

		/// <summary>Truncate mapper class.</summary>
		/// <remarks>
		/// Truncate mapper class.
		/// The mapper truncates given file to the newLength, specified by -size.
		/// </remarks>
		public class TruncateMapper : TestDFSIO.IOStatMapper
		{
			private const long Delay = 100L;

			private Path filePath;

			private long fileSize;

			/// <exception cref="System.IO.IOException"/>
			public override IDisposable GetIOStream(string name)
			{
				// IOMapperBase
				filePath = new Path(GetDataDir(GetConf()), name);
				fileSize = fs.GetFileStatus(filePath).GetLen();
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long DoIO(Reporter reporter, string name, long newLength)
			{
				// IOMapperBase
				// in bytes
				bool isClosed = fs.Truncate(filePath, newLength);
				reporter.SetStatus("truncating " + name + " to newLength " + newLength + " ::host = "
					 + hostName);
				for (int i = 0; !isClosed; i++)
				{
					try
					{
						Sharpen.Thread.Sleep(Delay);
					}
					catch (Exception)
					{
					}
					FileStatus status = fs.GetFileStatus(filePath);
					System.Diagnostics.Debug.Assert(status != null, "status is null");
					isClosed = (status.GetLen() == newLength);
					reporter.SetStatus("truncate recover for " + name + " to newLength " + newLength 
						+ " attempt " + i + " ::host = " + hostName);
				}
				return Sharpen.Extensions.ValueOf(fileSize - newLength);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TruncateTest(FileSystem fs)
		{
			Path TruncateDir = GetTruncateDir(config);
			fs.Delete(TruncateDir, true);
			RunIOTest(typeof(TestDFSIO.TruncateMapper), TruncateDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private void SequentialTest(FileSystem fs, TestDFSIO.TestType testType, long fileSize
			, int nrFiles)
		{
			// in bytes
			TestDFSIO.IOStatMapper ioer = null;
			switch (testType)
			{
				case TestDFSIO.TestType.TestTypeRead:
				{
					ioer = new TestDFSIO.ReadMapper();
					break;
				}

				case TestDFSIO.TestType.TestTypeWrite:
				{
					ioer = new TestDFSIO.WriteMapper();
					break;
				}

				case TestDFSIO.TestType.TestTypeAppend:
				{
					ioer = new TestDFSIO.AppendMapper();
					break;
				}

				case TestDFSIO.TestType.TestTypeReadRandom:
				case TestDFSIO.TestType.TestTypeReadBackward:
				case TestDFSIO.TestType.TestTypeReadSkip:
				{
					ioer = new TestDFSIO.RandomReadMapper();
					break;
				}

				case TestDFSIO.TestType.TestTypeTruncate:
				{
					ioer = new TestDFSIO.TruncateMapper();
					break;
				}

				default:
				{
					return;
				}
			}
			for (int i = 0; i < nrFiles; i++)
			{
				ioer.DoIO(Reporter.Null, BaseFileName + Sharpen.Extensions.ToString(i), fileSize);
			}
		}

		public static void Main(string[] args)
		{
			TestDFSIO bench = new TestDFSIO();
			int res = -1;
			try
			{
				res = ToolRunner.Run(bench, args);
			}
			catch (Exception e)
			{
				System.Console.Error.Write(StringUtils.StringifyException(e));
				res = -2;
			}
			if (res == -1)
			{
				System.Console.Error.Write(Usage);
			}
			System.Environment.Exit(res);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Run(string[] args)
		{
			// Tool
			TestDFSIO.TestType testType = null;
			int bufferSize = DefaultBufferSize;
			long nrBytes = 1 * Mega;
			int nrFiles = 1;
			long skipSize = 0;
			string resFileName = DefaultResFileName;
			string compressionClass = null;
			bool isSequential = false;
			string version = typeof(TestDFSIO).Name + ".1.8";
			Log.Info(version);
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine("Missing arguments.");
				return -1;
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].StartsWith("-read"))
				{
					testType = TestDFSIO.TestType.TestTypeRead;
				}
				else
				{
					if (args[i].Equals("-write"))
					{
						testType = TestDFSIO.TestType.TestTypeWrite;
					}
					else
					{
						if (args[i].Equals("-append"))
						{
							testType = TestDFSIO.TestType.TestTypeAppend;
						}
						else
						{
							if (args[i].Equals("-random"))
							{
								if (testType != TestDFSIO.TestType.TestTypeRead)
								{
									return -1;
								}
								testType = TestDFSIO.TestType.TestTypeReadRandom;
							}
							else
							{
								if (args[i].Equals("-backward"))
								{
									if (testType != TestDFSIO.TestType.TestTypeRead)
									{
										return -1;
									}
									testType = TestDFSIO.TestType.TestTypeReadBackward;
								}
								else
								{
									if (args[i].Equals("-skip"))
									{
										if (testType != TestDFSIO.TestType.TestTypeRead)
										{
											return -1;
										}
										testType = TestDFSIO.TestType.TestTypeReadSkip;
									}
									else
									{
										if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-truncate"))
										{
											testType = TestDFSIO.TestType.TestTypeTruncate;
										}
										else
										{
											if (args[i].Equals("-clean"))
											{
												testType = TestDFSIO.TestType.TestTypeCleanup;
											}
											else
											{
												if (args[i].StartsWith("-seq"))
												{
													isSequential = true;
												}
												else
												{
													if (args[i].StartsWith("-compression"))
													{
														compressionClass = args[++i];
													}
													else
													{
														if (args[i].Equals("-nrFiles"))
														{
															nrFiles = System.Convert.ToInt32(args[++i]);
														}
														else
														{
															if (args[i].Equals("-fileSize") || args[i].Equals("-size"))
															{
																nrBytes = ParseSize(args[++i]);
															}
															else
															{
																if (args[i].Equals("-skipSize"))
																{
																	skipSize = ParseSize(args[++i]);
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
																			System.Console.Error.WriteLine("Illegal argument: " + args[i]);
																			return -1;
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
					}
				}
			}
			if (testType == null)
			{
				return -1;
			}
			if (testType == TestDFSIO.TestType.TestTypeReadBackward)
			{
				skipSize = -bufferSize;
			}
			else
			{
				if (testType == TestDFSIO.TestType.TestTypeReadSkip && skipSize == 0)
				{
					skipSize = bufferSize;
				}
			}
			Log.Info("nrFiles = " + nrFiles);
			Log.Info("nrBytes (MB) = " + ToMB(nrBytes));
			Log.Info("bufferSize = " + bufferSize);
			if (skipSize > 0)
			{
				Log.Info("skipSize = " + skipSize);
			}
			Log.Info("baseDir = " + GetBaseDir(config));
			if (compressionClass != null)
			{
				config.Set("test.io.compression.class", compressionClass);
				Log.Info("compressionClass = " + compressionClass);
			}
			config.SetInt("test.io.file.buffer.size", bufferSize);
			config.SetLong("test.io.skip.size", skipSize);
			config.SetBoolean(DFSConfigKeys.DfsSupportAppendKey, true);
			FileSystem fs = FileSystem.Get(config);
			if (isSequential)
			{
				long tStart = Runtime.CurrentTimeMillis();
				SequentialTest(fs, testType, nrBytes, nrFiles);
				long execTime = Runtime.CurrentTimeMillis() - tStart;
				string resultLine = "Seq Test exec time sec: " + (float)execTime / 1000;
				Log.Info(resultLine);
				return 0;
			}
			if (testType == TestDFSIO.TestType.TestTypeCleanup)
			{
				Cleanup(fs);
				return 0;
			}
			CreateControlFile(fs, nrBytes, nrFiles);
			long tStart_1 = Runtime.CurrentTimeMillis();
			switch (testType)
			{
				case TestDFSIO.TestType.TestTypeWrite:
				{
					WriteTest(fs);
					break;
				}

				case TestDFSIO.TestType.TestTypeRead:
				{
					ReadTest(fs);
					break;
				}

				case TestDFSIO.TestType.TestTypeAppend:
				{
					AppendTest(fs);
					break;
				}

				case TestDFSIO.TestType.TestTypeReadRandom:
				case TestDFSIO.TestType.TestTypeReadBackward:
				case TestDFSIO.TestType.TestTypeReadSkip:
				{
					RandomReadTest(fs);
					break;
				}

				case TestDFSIO.TestType.TestTypeTruncate:
				{
					TruncateTest(fs);
					break;
				}

				default:
				{
					break;
				}
			}
			long execTime_1 = Runtime.CurrentTimeMillis() - tStart_1;
			AnalyzeResult(fs, testType, execTime_1, resFileName);
			return 0;
		}

		public virtual Configuration GetConf()
		{
			// Configurable
			return this.config;
		}

		public virtual void SetConf(Configuration conf)
		{
			// Configurable
			this.config = conf;
		}

		/// <summary>Returns size in bytes.</summary>
		/// <param name="arg">= {d}[B|KB|MB|GB|TB]</param>
		/// <returns/>
		internal static long ParseSize(string arg)
		{
			string[] args = arg.Split("\\D", 2);
			// get digits
			System.Diagnostics.Debug.Assert(args.Length <= 2);
			long nrBytes = long.Parse(args[0]);
			string bytesMult = Sharpen.Runtime.Substring(arg, args[0].Length);
			// get byte multiple
			return nrBytes * TestDFSIO.ByteMultiple.ParseString(bytesMult).Value();
		}

		internal static float ToMB(long bytes)
		{
			return ((float)bytes) / Mega;
		}

		/// <exception cref="System.IO.IOException"/>
		private void AnalyzeResult(FileSystem fs, TestDFSIO.TestType testType, long execTime
			, string resFileName)
		{
			Path reduceFile = GetReduceFilePath(testType);
			long tasks = 0;
			long size = 0;
			long time = 0;
			float rate = 0;
			float sqrate = 0;
			DataInputStream @in = null;
			BufferedReader lines = null;
			try
			{
				@in = new DataInputStream(fs.Open(reduceFile));
				lines = new BufferedReader(new InputStreamReader(@in));
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
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
				if (lines != null)
				{
					lines.Close();
				}
			}
			double med = rate / 1000 / tasks;
			double stdDev = Math.Sqrt(Math.Abs(sqrate / 1000 / tasks - med * med));
			string[] resultLines = new string[] { "----- TestDFSIO ----- : " + testType, "           Date & time: "
				 + Sharpen.Extensions.CreateDate(Runtime.CurrentTimeMillis()), "       Number of files: "
				 + tasks, "Total MBytes processed: " + ToMB(size), "     Throughput mb/sec: " + 
				size * 1000.0 / (time * Mega), "Average IO rate mb/sec: " + med, " IO rate std deviation: "
				 + stdDev, "    Test exec time sec: " + (float)execTime / 1000, string.Empty };
			TextWriter res = null;
			try
			{
				res = new TextWriter(new FileOutputStream(new FilePath(resFileName), true));
				for (int i = 0; i < resultLines.Length; i++)
				{
					Log.Info(resultLines[i]);
					res.WriteLine(resultLines[i]);
				}
			}
			finally
			{
				if (res != null)
				{
					res.Close();
				}
			}
		}

		private Path GetReduceFilePath(TestDFSIO.TestType testType)
		{
			switch (testType)
			{
				case TestDFSIO.TestType.TestTypeWrite:
				{
					return new Path(GetWriteDir(config), "part-00000");
				}

				case TestDFSIO.TestType.TestTypeAppend:
				{
					return new Path(GetAppendDir(config), "part-00000");
				}

				case TestDFSIO.TestType.TestTypeRead:
				{
					return new Path(GetReadDir(config), "part-00000");
				}

				case TestDFSIO.TestType.TestTypeReadRandom:
				case TestDFSIO.TestType.TestTypeReadBackward:
				case TestDFSIO.TestType.TestTypeReadSkip:
				{
					return new Path(GetRandomReadDir(config), "part-00000");
				}

				case TestDFSIO.TestType.TestTypeTruncate:
				{
					return new Path(GetTruncateDir(config), "part-00000");
				}

				default:
				{
					break;
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		private void AnalyzeResult(FileSystem fs, TestDFSIO.TestType testType, long execTime
			)
		{
			string dir = Runtime.GetProperty("test.build.dir", "target/test-dir");
			AnalyzeResult(fs, testType, execTime, dir + "/" + DefaultResFileName);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Cleanup(FileSystem fs)
		{
			Log.Info("Cleaning up test files");
			fs.Delete(new Path(GetBaseDir(config)), true);
		}
	}
}
