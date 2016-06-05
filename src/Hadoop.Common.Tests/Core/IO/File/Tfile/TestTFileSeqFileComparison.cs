using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestTFileSeqFileComparison : TestCase
	{
		internal TestTFileSeqFileComparison.MyOptions options;

		private FileSystem fs;

		private Configuration conf;

		private long startTimeEpoch;

		private long finishTimeEpoch;

		private DateFormat formatter;

		internal byte[][] dictionary;

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			if (options == null)
			{
				options = new TestTFileSeqFileComparison.MyOptions(new string[0]);
			}
			conf = new Configuration();
			conf.SetInt("tfile.fs.input.buffer.size", options.fsInputBufferSize);
			conf.SetInt("tfile.fs.output.buffer.size", options.fsOutputBufferSize);
			Path path = new Path(options.rootDir);
			fs = path.GetFileSystem(conf);
			formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SetUpDictionary();
		}

		private void SetUpDictionary()
		{
			Random rng = new Random();
			dictionary = new byte[options.dictSize][];
			for (int i = 0; i < options.dictSize; ++i)
			{
				int len = rng.Next(options.maxWordLen - options.minWordLen) + options.minWordLen;
				dictionary[i] = new byte[len];
				rng.NextBytes(dictionary[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
		}

		// do nothing
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartTime()
		{
			startTimeEpoch = Time.Now();
			System.Console.Out.WriteLine(FormatTime() + " Started timing.");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StopTime()
		{
			finishTimeEpoch = Time.Now();
			System.Console.Out.WriteLine(FormatTime() + " Stopped timing.");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetIntervalMillis()
		{
			return finishTimeEpoch - startTimeEpoch;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PrintlnWithTimestamp(string message)
		{
			System.Console.Out.WriteLine(FormatTime() + "  " + message);
		}

		/*
		* Format millis into minutes and seconds.
		*/
		public virtual string FormatTime(long milis)
		{
			return formatter.Format(milis);
		}

		public virtual string FormatTime()
		{
			return FormatTime(Time.Now());
		}

		private interface KVAppendable
		{
			/// <exception cref="System.IO.IOException"/>
			void Append(BytesWritable key, BytesWritable value);

			/// <exception cref="System.IO.IOException"/>
			void Close();
		}

		private interface KVReadable
		{
			byte[] GetKey();

			byte[] GetValue();

			int GetKeyLength();

			int GetValueLength();

			/// <exception cref="System.IO.IOException"/>
			bool Next();

			/// <exception cref="System.IO.IOException"/>
			void Close();
		}

		internal class TFileAppendable : TestTFileSeqFileComparison.KVAppendable
		{
			private FSDataOutputStream fsdos;

			private TFile.Writer writer;

			/// <exception cref="System.IO.IOException"/>
			public TFileAppendable(FileSystem fs, Path path, string compress, int minBlkSize, 
				int osBufferSize, Configuration conf)
			{
				this.fsdos = fs.Create(path, true, osBufferSize);
				this.writer = new TFile.Writer(fsdos, minBlkSize, compress, null, conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(BytesWritable key, BytesWritable value)
			{
				writer.Append(key.Get(), 0, key.GetSize(), value.Get(), 0, value.GetSize());
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				writer.Close();
				fsdos.Close();
			}
		}

		internal class TFileReadable : TestTFileSeqFileComparison.KVReadable
		{
			private FSDataInputStream fsdis;

			private TFile.Reader reader;

			private TFile.Reader.Scanner scanner;

			private byte[] keyBuffer;

			private int keyLength;

			private byte[] valueBuffer;

			private int valueLength;

			/// <exception cref="System.IO.IOException"/>
			public TFileReadable(FileSystem fs, Path path, int osBufferSize, Configuration conf
				)
			{
				this.fsdis = fs.Open(path, osBufferSize);
				this.reader = new TFile.Reader(fsdis, fs.GetFileStatus(path).GetLen(), conf);
				this.scanner = reader.CreateScanner();
				keyBuffer = new byte[32];
				valueBuffer = new byte[32];
			}

			private void CheckKeyBuffer(int size)
			{
				if (size <= keyBuffer.Length)
				{
					return;
				}
				keyBuffer = new byte[Math.Max(2 * keyBuffer.Length, 2 * size - keyBuffer.Length)]
					;
			}

			private void CheckValueBuffer(int size)
			{
				if (size <= valueBuffer.Length)
				{
					return;
				}
				valueBuffer = new byte[Math.Max(2 * valueBuffer.Length, 2 * size - valueBuffer.Length
					)];
			}

			public virtual byte[] GetKey()
			{
				return keyBuffer;
			}

			public virtual int GetKeyLength()
			{
				return keyLength;
			}

			public virtual byte[] GetValue()
			{
				return valueBuffer;
			}

			public virtual int GetValueLength()
			{
				return valueLength;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next()
			{
				if (scanner.AtEnd())
				{
					return false;
				}
				TFile.Reader.Scanner.Entry entry = scanner.Entry();
				keyLength = entry.GetKeyLength();
				CheckKeyBuffer(keyLength);
				entry.GetKey(keyBuffer);
				valueLength = entry.GetValueLength();
				CheckValueBuffer(valueLength);
				entry.GetValue(valueBuffer);
				scanner.Advance();
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				scanner.Close();
				reader.Close();
				fsdis.Close();
			}
		}

		internal class SeqFileAppendable : TestTFileSeqFileComparison.KVAppendable
		{
			private FSDataOutputStream fsdos;

			private SequenceFile.Writer writer;

			/// <exception cref="System.IO.IOException"/>
			public SeqFileAppendable(FileSystem fs, Path path, int osBufferSize, string compress
				, int minBlkSize)
			{
				Configuration conf = new Configuration();
				conf.SetBoolean("hadoop.native.lib", true);
				CompressionCodec codec = null;
				if ("lzo".Equals(compress))
				{
					codec = Compression.Algorithm.Lzo.GetCodec();
				}
				else
				{
					if ("gz".Equals(compress))
					{
						codec = Compression.Algorithm.Gz.GetCodec();
					}
					else
					{
						if (!"none".Equals(compress))
						{
							throw new IOException("Codec not supported.");
						}
					}
				}
				this.fsdos = fs.Create(path, true, osBufferSize);
				if (!"none".Equals(compress))
				{
					writer = SequenceFile.CreateWriter(conf, fsdos, typeof(BytesWritable), typeof(BytesWritable
						), SequenceFile.CompressionType.Block, codec);
				}
				else
				{
					writer = SequenceFile.CreateWriter(conf, fsdos, typeof(BytesWritable), typeof(BytesWritable
						), SequenceFile.CompressionType.None, null);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(BytesWritable key, BytesWritable value)
			{
				writer.Append(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				writer.Close();
				fsdos.Close();
			}
		}

		internal class SeqFileReadable : TestTFileSeqFileComparison.KVReadable
		{
			private SequenceFile.Reader reader;

			private BytesWritable key;

			private BytesWritable value;

			/// <exception cref="System.IO.IOException"/>
			public SeqFileReadable(FileSystem fs, Path path, int osBufferSize)
			{
				Configuration conf = new Configuration();
				conf.SetInt("io.file.buffer.size", osBufferSize);
				reader = new SequenceFile.Reader(fs, path, conf);
				key = new BytesWritable();
				value = new BytesWritable();
			}

			public virtual byte[] GetKey()
			{
				return key.Get();
			}

			public virtual int GetKeyLength()
			{
				return key.GetSize();
			}

			public virtual byte[] GetValue()
			{
				return value.Get();
			}

			public virtual int GetValueLength()
			{
				return value.GetSize();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next()
			{
				return reader.Next(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				reader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReportStats(Path path, long totalBytes)
		{
			long duration = GetIntervalMillis();
			long fsize = fs.GetFileStatus(path).GetLen();
			PrintlnWithTimestamp(string.Format("Duration: %dms...total size: %.2fMB...raw thrpt: %.2fMB/s"
				, duration, (double)totalBytes / 1024 / 1024, (double)totalBytes / duration * 1000
				 / 1024 / 1024));
			PrintlnWithTimestamp(string.Format("Compressed size: %.2fMB...compressed thrpt: %.2fMB/s."
				, (double)fsize / 1024 / 1024, (double)fsize / duration * 1000 / 1024 / 1024));
		}

		private void FillBuffer(Random rng, BytesWritable bw, byte[] tmp, int len)
		{
			int n = 0;
			while (n < len)
			{
				byte[] word = dictionary[rng.Next(dictionary.Length)];
				int l = Math.Min(word.Length, len - n);
				System.Array.Copy(word, 0, tmp, n, l);
				n += l;
			}
			bw.Set(tmp, 0, len);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TimeWrite(Path path, TestTFileSeqFileComparison.KVAppendable appendable
			, int baseKlen, int baseVlen, long fileSize)
		{
			int maxKlen = baseKlen * 2;
			int maxVlen = baseVlen * 2;
			BytesWritable key = new BytesWritable();
			BytesWritable value = new BytesWritable();
			byte[] keyBuffer = new byte[maxKlen];
			byte[] valueBuffer = new byte[maxVlen];
			Random rng = new Random(options.seed);
			long totalBytes = 0;
			PrintlnWithTimestamp("Start writing: " + path.GetName() + "...");
			StartTime();
			for (long i = 0; true; ++i)
			{
				if (i % 1000 == 0)
				{
					// test the size for every 1000 rows.
					if (fs.GetFileStatus(path).GetLen() >= fileSize)
					{
						break;
					}
				}
				int klen = rng.Next(baseKlen) + baseKlen;
				int vlen = rng.Next(baseVlen) + baseVlen;
				FillBuffer(rng, key, keyBuffer, klen);
				FillBuffer(rng, value, valueBuffer, vlen);
				key.Set(keyBuffer, 0, klen);
				value.Set(valueBuffer, 0, vlen);
				appendable.Append(key, value);
				totalBytes += klen;
				totalBytes += vlen;
			}
			StopTime();
			appendable.Close();
			ReportStats(path, totalBytes);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TimeRead(Path path, TestTFileSeqFileComparison.KVReadable readable)
		{
			PrintlnWithTimestamp("Start reading: " + path.GetName() + "...");
			long totalBytes = 0;
			StartTime();
			for (; readable.Next(); )
			{
				totalBytes += readable.GetKeyLength();
				totalBytes += readable.GetValueLength();
			}
			StopTime();
			readable.Close();
			ReportStats(path, totalBytes);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateTFile(string parameters, string compress)
		{
			System.Console.Out.WriteLine("=== TFile: Creation (" + parameters + ") === ");
			Path path = new Path(options.rootDir, "TFile.Performance");
			TestTFileSeqFileComparison.KVAppendable appendable = new TestTFileSeqFileComparison.TFileAppendable
				(fs, path, compress, options.minBlockSize, options.osOutputBufferSize, conf);
			TimeWrite(path, appendable, options.keyLength, options.valueLength, options.fileSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadTFile(string parameters, bool delFile)
		{
			System.Console.Out.WriteLine("=== TFile: Reading (" + parameters + ") === ");
			{
				Path path = new Path(options.rootDir, "TFile.Performance");
				TestTFileSeqFileComparison.KVReadable readable = new TestTFileSeqFileComparison.TFileReadable
					(fs, path, options.osInputBufferSize, conf);
				TimeRead(path, readable);
				if (delFile)
				{
					if (fs.Exists(path))
					{
						fs.Delete(path, true);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateSeqFile(string parameters, string compress)
		{
			System.Console.Out.WriteLine("=== SeqFile: Creation (" + parameters + ") === ");
			Path path = new Path(options.rootDir, "SeqFile.Performance");
			TestTFileSeqFileComparison.KVAppendable appendable = new TestTFileSeqFileComparison.SeqFileAppendable
				(fs, path, options.osOutputBufferSize, compress, options.minBlockSize);
			TimeWrite(path, appendable, options.keyLength, options.valueLength, options.fileSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadSeqFile(string parameters, bool delFile)
		{
			System.Console.Out.WriteLine("=== SeqFile: Reading (" + parameters + ") === ");
			Path path = new Path(options.rootDir, "SeqFile.Performance");
			TestTFileSeqFileComparison.KVReadable readable = new TestTFileSeqFileComparison.SeqFileReadable
				(fs, path, options.osInputBufferSize);
			TimeRead(path, readable);
			if (delFile)
			{
				if (fs.Exists(path))
				{
					fs.Delete(path, true);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CompareRun(string compress)
		{
			string[] supported = TFile.GetSupportedCompressionAlgorithms();
			bool proceed = false;
			foreach (string c in supported)
			{
				if (c.Equals(compress))
				{
					proceed = true;
					break;
				}
			}
			if (!proceed)
			{
				System.Console.Out.WriteLine("Skipped for " + compress);
				return;
			}
			options.compress = compress;
			string parameters = Parameters2String(options);
			CreateSeqFile(parameters, compress);
			ReadSeqFile(parameters, true);
			CreateTFile(parameters, compress);
			ReadTFile(parameters, true);
			CreateTFile(parameters, compress);
			ReadTFile(parameters, true);
			CreateSeqFile(parameters, compress);
			ReadSeqFile(parameters, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRunComparisons()
		{
			string[] compresses = new string[] { "none", "lzo", "gz" };
			foreach (string compress in compresses)
			{
				if (compress.Equals("none"))
				{
					conf.SetInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeNone);
					conf.SetInt("tfile.fs.output.buffer.size", options.fsOutputBufferSizeNone);
				}
				else
				{
					if (compress.Equals("lzo"))
					{
						conf.SetInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeLzo);
						conf.SetInt("tfile.fs.output.buffer.size", options.fsOutputBufferSizeLzo);
					}
					else
					{
						conf.SetInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeGz);
						conf.SetInt("tfile.fs.output.buffer.size", options.fsOutputBufferSizeGz);
					}
				}
				CompareRun(compress);
			}
		}

		private static string Parameters2String(TestTFileSeqFileComparison.MyOptions options
			)
		{
			return string.Format("KLEN: %d-%d... VLEN: %d-%d...MinBlkSize: %.2fKB...Target Size: %.2fMB...Compression: ...%s"
				, options.keyLength, options.keyLength * 2, options.valueLength, options.valueLength
				 * 2, (double)options.minBlockSize / 1024, (double)options.fileSize / 1024 / 1024
				, options.compress);
		}

		private class MyOptions
		{
			internal string rootDir = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
				);

			internal string compress = "gz";

			internal string format = "tfile";

			internal int dictSize = 1000;

			internal int minWordLen = 5;

			internal int maxWordLen = 20;

			internal int keyLength = 50;

			internal int valueLength = 100;

			internal int minBlockSize = 256 * 1024;

			internal int fsOutputBufferSize = 1;

			internal int fsInputBufferSize = 0;

			internal int fsInputBufferSizeNone = 0;

			internal int fsInputBufferSizeGz = 0;

			internal int fsInputBufferSizeLzo = 0;

			internal int fsOutputBufferSizeNone = 1;

			internal int fsOutputBufferSizeGz = 1;

			internal int fsOutputBufferSizeLzo = 1;

			internal int osInputBufferSize = 64 * 1024;

			internal int osOutputBufferSize = 64 * 1024;

			internal long fileSize = 3 * 1024 * 1024;

			internal long seed;

			internal const int OpCreate = 1;

			internal const int OpRead = 2;

			internal int op = OpRead;

			internal bool proceed = false;

			public MyOptions(string[] args)
			{
				// special variable only for unit testing.
				// un-exposed parameters.
				seed = Runtime.NanoTime();
				try
				{
					Options opts = BuildOptions();
					CommandLineParser parser = new GnuParser();
					CommandLine line = parser.Parse(opts, args, true);
					ProcessOptions(line, opts);
					ValidateOptions();
				}
				catch (ParseException e)
				{
					System.Console.Out.WriteLine(e.Message);
					System.Console.Out.WriteLine("Try \"--help\" option for details.");
					SetStopProceed();
				}
			}

			public virtual bool Proceed()
			{
				return proceed;
			}

			private Options BuildOptions()
			{
				Option compress = OptionBuilder.Create('c');
				Option ditSize = OptionBuilder.Create('d');
				Option fileSize = OptionBuilder.Create('s');
				Option format = OptionBuilder.Create('f');
				Option fsInputBufferSz = OptionBuilder.Create('i');
				Option fsOutputBufferSize = OptionBuilder.Create('o');
				Option keyLen = OptionBuilder.Create('k');
				Option valueLen = OptionBuilder.Create('v');
				Option wordLen = OptionBuilder.Create('w');
				Option blockSz = OptionBuilder.Create('b');
				Option seed = OptionBuilder.Create('S');
				Option operation = OptionBuilder.Create('x');
				Option rootDir = OptionBuilder.Create('r');
				Option help = OptionBuilder.Create("h");
				return new Options().AddOption(compress).AddOption(ditSize).AddOption(fileSize).AddOption
					(format).AddOption(fsInputBufferSz).AddOption(fsOutputBufferSize).AddOption(keyLen
					).AddOption(wordLen).AddOption(blockSz).AddOption(rootDir).AddOption(valueLen).AddOption
					(operation).AddOption(help);
			}

			/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
			private void ProcessOptions(CommandLine line, Options opts)
			{
				// --help -h and --version -V must be processed first.
				if (line.HasOption('h'))
				{
					HelpFormatter formatter = new HelpFormatter();
					System.Console.Out.WriteLine("TFile and SeqFile benchmark.");
					System.Console.Out.WriteLine();
					formatter.PrintHelp(100, "java ... TestTFileSeqFileComparison [options]", "\nSupported options:"
						, opts, string.Empty);
					return;
				}
				if (line.HasOption('c'))
				{
					compress = line.GetOptionValue('c');
				}
				if (line.HasOption('d'))
				{
					dictSize = System.Convert.ToInt32(line.GetOptionValue('d'));
				}
				if (line.HasOption('s'))
				{
					fileSize = long.Parse(line.GetOptionValue('s')) * 1024 * 1024;
				}
				if (line.HasOption('f'))
				{
					format = line.GetOptionValue('f');
				}
				if (line.HasOption('i'))
				{
					fsInputBufferSize = System.Convert.ToInt32(line.GetOptionValue('i'));
				}
				if (line.HasOption('o'))
				{
					fsOutputBufferSize = System.Convert.ToInt32(line.GetOptionValue('o'));
				}
				if (line.HasOption('k'))
				{
					keyLength = System.Convert.ToInt32(line.GetOptionValue('k'));
				}
				if (line.HasOption('v'))
				{
					valueLength = System.Convert.ToInt32(line.GetOptionValue('v'));
				}
				if (line.HasOption('b'))
				{
					minBlockSize = System.Convert.ToInt32(line.GetOptionValue('b')) * 1024;
				}
				if (line.HasOption('r'))
				{
					rootDir = line.GetOptionValue('r');
				}
				if (line.HasOption('S'))
				{
					seed = long.Parse(line.GetOptionValue('S'));
				}
				if (line.HasOption('w'))
				{
					string min_max = line.GetOptionValue('w');
					StringTokenizer st = new StringTokenizer(min_max, " \t,");
					if (st.CountTokens() != 2)
					{
						throw new ParseException("Bad word length specification: " + min_max);
					}
					minWordLen = System.Convert.ToInt32(st.NextToken());
					maxWordLen = System.Convert.ToInt32(st.NextToken());
				}
				if (line.HasOption('x'))
				{
					string strOp = line.GetOptionValue('x');
					if (strOp.Equals("r"))
					{
						op = OpRead;
					}
					else
					{
						if (strOp.Equals("w"))
						{
							op = OpCreate;
						}
						else
						{
							if (strOp.Equals("rw"))
							{
								op = OpCreate | OpRead;
							}
							else
							{
								throw new ParseException("Unknown action specifier: " + strOp);
							}
						}
					}
				}
				proceed = true;
			}

			/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
			private void ValidateOptions()
			{
				if (!compress.Equals("none") && !compress.Equals("lzo") && !compress.Equals("gz"))
				{
					throw new ParseException("Unknown compression scheme: " + compress);
				}
				if (!format.Equals("tfile") && !format.Equals("seqfile"))
				{
					throw new ParseException("Unknown file format: " + format);
				}
				if (minWordLen >= maxWordLen)
				{
					throw new ParseException("Max word length must be greater than min word length.");
				}
				return;
			}

			private void SetStopProceed()
			{
				proceed = false;
			}

			public virtual bool DoCreate()
			{
				return (op & OpCreate) != 0;
			}

			public virtual bool DoRead()
			{
				return (op & OpRead) != 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			TestTFileSeqFileComparison testCase = new TestTFileSeqFileComparison();
			TestTFileSeqFileComparison.MyOptions options = new TestTFileSeqFileComparison.MyOptions
				(args);
			if (options.proceed == false)
			{
				return;
			}
			testCase.options = options;
			string parameters = Parameters2String(options);
			testCase.SetUp();
			if (testCase.options.format.Equals("tfile"))
			{
				if (options.DoCreate())
				{
					testCase.CreateTFile(parameters, options.compress);
				}
				if (options.DoRead())
				{
					testCase.ReadTFile(parameters, options.DoCreate());
				}
			}
			else
			{
				if (options.DoCreate())
				{
					testCase.CreateSeqFile(parameters, options.compress);
				}
				if (options.DoRead())
				{
					testCase.ReadSeqFile(parameters, options.DoCreate());
				}
			}
			testCase.TearDown();
		}
	}
}
