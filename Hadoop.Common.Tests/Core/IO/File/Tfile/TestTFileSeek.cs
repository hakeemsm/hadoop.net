using NUnit.Framework;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>test the performance for seek.</summary>
	public class TestTFileSeek : TestCase
	{
		private TestTFileSeek.MyOptions options;

		private Configuration conf;

		private Path path;

		private FileSystem fs;

		private NanoTimer timer;

		private Random rng;

		private RandomDistribution.DiscreteRNG keyLenGen;

		private KVGenerator kvGen;

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			if (options == null)
			{
				options = new TestTFileSeek.MyOptions(new string[0]);
			}
			conf = new Configuration();
			conf.SetInt("tfile.fs.input.buffer.size", options.fsInputBufferSize);
			conf.SetInt("tfile.fs.output.buffer.size", options.fsOutputBufferSize);
			path = new Path(new Path(options.rootDir), options.file);
			fs = path.GetFileSystem(conf);
			timer = new NanoTimer(false);
			rng = new Random(options.seed);
			keyLenGen = new RandomDistribution.Zipf(new Random(rng.NextLong()), options.minKeyLen
				, options.maxKeyLen, 1.2);
			RandomDistribution.DiscreteRNG valLenGen = new RandomDistribution.Flat(new Random
				(rng.NextLong()), options.minValLength, options.maxValLength);
			RandomDistribution.DiscreteRNG wordLenGen = new RandomDistribution.Flat(new Random
				(rng.NextLong()), options.minWordLen, options.maxWordLen);
			kvGen = new KVGenerator(rng, true, keyLenGen, valLenGen, wordLenGen, options.dictSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			fs.Delete(path, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static FSDataOutputStream CreateFSOutput(Path name, FileSystem fs)
		{
			if (fs.Exists(name))
			{
				fs.Delete(name, true);
			}
			FSDataOutputStream fout = fs.Create(name);
			return fout;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateTFile()
		{
			long totalBytes = 0;
			FSDataOutputStream fout = CreateFSOutput(path, fs);
			try
			{
				TFile.Writer writer = new TFile.Writer(fout, options.minBlockSize, options.compress
					, "memcmp", conf);
				try
				{
					BytesWritable key = new BytesWritable();
					BytesWritable val = new BytesWritable();
					timer.Start();
					for (long i = 0; true; ++i)
					{
						if (i % 1000 == 0)
						{
							// test the size for every 1000 rows.
							if (fs.GetFileStatus(path).GetLen() >= options.fileSize)
							{
								break;
							}
						}
						kvGen.Next(key, val, false);
						writer.Append(key.Get(), 0, key.GetSize(), val.Get(), 0, val.GetSize());
						totalBytes += key.GetSize();
						totalBytes += val.GetSize();
					}
					timer.Stop();
				}
				finally
				{
					writer.Close();
				}
			}
			finally
			{
				fout.Close();
			}
			double duration = (double)timer.Read() / 1000;
			// in us.
			long fsize = fs.GetFileStatus(path).GetLen();
			System.Console.Out.Printf("time: %s...uncompressed: %.2fMB...raw thrpt: %.2fMB/s\n"
				, timer.ToString(), (double)totalBytes / 1024 / 1024, totalBytes / duration);
			System.Console.Out.Printf("time: %s...file size: %.2fMB...disk thrpt: %.2fMB/s\n"
				, timer.ToString(), (double)fsize / 1024 / 1024, fsize / duration);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SeekTFile()
		{
			int miss = 0;
			long totalBytes = 0;
			FSDataInputStream fsdis = fs.Open(path);
			TFile.Reader reader = new TFile.Reader(fsdis, fs.GetFileStatus(path).GetLen(), conf
				);
			KeySampler kSampler = new KeySampler(rng, reader.GetFirstKey(), reader.GetLastKey
				(), keyLenGen);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			BytesWritable key = new BytesWritable();
			BytesWritable val = new BytesWritable();
			timer.Reset();
			timer.Start();
			for (int i = 0; i < options.seekCount; ++i)
			{
				kSampler.Next(key);
				scanner.LowerBound(key.Get(), 0, key.GetSize());
				if (!scanner.AtEnd())
				{
					scanner.Entry().Get(key, val);
					totalBytes += key.GetSize();
					totalBytes += val.GetSize();
				}
				else
				{
					++miss;
				}
			}
			timer.Stop();
			double duration = (double)timer.Read() / 1000;
			// in us.
			System.Console.Out.Printf("time: %s...avg seek: %s...%d hit...%d miss...avg I/O size: %.2fKB\n"
				, timer.ToString(), NanoTimer.NanoTimeToString(timer.Read() / options.seekCount)
				, options.seekCount - miss, miss, (double)totalBytes / 1024 / (options.seekCount
				 - miss));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSeeks()
		{
			string[] supported = TFile.GetSupportedCompressionAlgorithms();
			bool proceed = false;
			foreach (string c in supported)
			{
				if (c.Equals(options.compress))
				{
					proceed = true;
					break;
				}
			}
			if (!proceed)
			{
				System.Console.Out.WriteLine("Skipped for " + options.compress);
				return;
			}
			if (options.DoCreate())
			{
				CreateTFile();
			}
			if (options.DoRead())
			{
				SeekTFile();
			}
		}

		private class IntegerRange
		{
			private readonly int from;

			private readonly int to;

			public IntegerRange(int from, int to)
			{
				this.from = from;
				this.to = to;
			}

			/// <exception cref="Org.Apache.Commons.Cli.ParseException"/>
			public static TestTFileSeek.IntegerRange Parse(string s)
			{
				StringTokenizer st = new StringTokenizer(s, " \t,");
				if (st.CountTokens() != 2)
				{
					throw new ParseException("Bad integer specification: " + s);
				}
				int from = System.Convert.ToInt32(st.NextToken());
				int to = System.Convert.ToInt32(st.NextToken());
				return new TestTFileSeek.IntegerRange(from, to);
			}

			public virtual int From()
			{
				return from;
			}

			public virtual int To()
			{
				return to;
			}
		}

		private class MyOptions
		{
			internal int dictSize = 1000;

			internal int minWordLen = 5;

			internal int maxWordLen = 20;

			internal int osInputBufferSize = 64 * 1024;

			internal int osOutputBufferSize = 64 * 1024;

			internal int fsInputBufferSizeNone = 0;

			internal int fsInputBufferSizeLzo = 0;

			internal int fsInputBufferSizeGz = 0;

			internal int fsOutputBufferSizeNone = 1;

			internal int fsOutputBufferSizeLzo = 1;

			internal int fsOutputBufferSizeGz = 1;

			internal string rootDir = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
				);

			internal string file = "TestTFileSeek";

			internal string compress = "gz";

			internal int minKeyLen = 10;

			internal int maxKeyLen = 50;

			internal int minValLength = 100;

			internal int maxValLength = 200;

			internal int minBlockSize = 64 * 1024;

			internal int fsOutputBufferSize = 1;

			internal int fsInputBufferSize = 0;

			internal long fileSize = 3 * 1024 * 1024;

			internal long seekCount = 1000;

			internal long seed;

			internal const int OpCreate = 1;

			internal const int OpRead = 2;

			internal int op = OpCreate | OpRead;

			internal bool proceed = false;

			public MyOptions(string[] args)
			{
				// hard coded constants
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
				Option fileSize = OptionBuilder.Create('s');
				Option fsInputBufferSz = OptionBuilder.Create('i');
				Option fsOutputBufferSize = OptionBuilder.Create('o');
				Option keyLen = OptionBuilder.Create('k');
				Option valueLen = OptionBuilder.Create('v');
				Option blockSz = OptionBuilder.Create('b');
				Option seed = OptionBuilder.Create('S');
				Option operation = OptionBuilder.Create('x');
				Option rootDir = OptionBuilder.Create('r');
				Option file = OptionBuilder.Create('f');
				Option seekCount = OptionBuilder.Create('n');
				Option help = OptionBuilder.Create("h");
				return new Options().AddOption(compress).AddOption(fileSize).AddOption(fsInputBufferSz
					).AddOption(fsOutputBufferSize).AddOption(keyLen).AddOption(blockSz).AddOption(rootDir
					).AddOption(valueLen).AddOption(operation).AddOption(seekCount).AddOption(file).
					AddOption(help);
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
				if (line.HasOption('i'))
				{
					fsInputBufferSize = System.Convert.ToInt32(line.GetOptionValue('i'));
				}
				if (line.HasOption('o'))
				{
					fsOutputBufferSize = System.Convert.ToInt32(line.GetOptionValue('o'));
				}
				if (line.HasOption('n'))
				{
					seekCount = System.Convert.ToInt32(line.GetOptionValue('n'));
				}
				if (line.HasOption('k'))
				{
					TestTFileSeek.IntegerRange ir = TestTFileSeek.IntegerRange.Parse(line.GetOptionValue
						('k'));
					minKeyLen = ir.From();
					maxKeyLen = ir.To();
				}
				if (line.HasOption('v'))
				{
					TestTFileSeek.IntegerRange ir = TestTFileSeek.IntegerRange.Parse(line.GetOptionValue
						('v'));
					minValLength = ir.From();
					maxValLength = ir.To();
				}
				if (line.HasOption('b'))
				{
					minBlockSize = System.Convert.ToInt32(line.GetOptionValue('b')) * 1024;
				}
				if (line.HasOption('r'))
				{
					rootDir = line.GetOptionValue('r');
				}
				if (line.HasOption('f'))
				{
					file = line.GetOptionValue('f');
				}
				if (line.HasOption('S'))
				{
					seed = long.Parse(line.GetOptionValue('S'));
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
				if (minKeyLen >= maxKeyLen)
				{
					throw new ParseException("Max key length must be greater than min key length.");
				}
				if (minValLength >= maxValLength)
				{
					throw new ParseException("Max value length must be greater than min value length."
						);
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
		public static void Main(string[] argv)
		{
			TestTFileSeek testCase = new TestTFileSeek();
			TestTFileSeek.MyOptions options = new TestTFileSeek.MyOptions(argv);
			if (options.proceed == false)
			{
				return;
			}
			testCase.options = options;
			testCase.SetUp();
			testCase.TestSeeks();
			testCase.TearDown();
		}
	}
}
