using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>test the performance for seek.</summary>
	public class TestTFileSeek : NUnit.Framework.TestCase
	{
		private org.apache.hadoop.io.file.tfile.TestTFileSeek.MyOptions options;

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.Path path;

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.io.file.tfile.NanoTimer timer;

		private java.util.Random rng;

		private org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG keyLenGen;

		private org.apache.hadoop.io.file.tfile.KVGenerator kvGen;

		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			if (options == null)
			{
				options = new org.apache.hadoop.io.file.tfile.TestTFileSeek.MyOptions(new string[
					0]);
			}
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSize);
			conf.setInt("tfile.fs.output.buffer.size", options.fsOutputBufferSize);
			path = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path(options.rootDir
				), options.file);
			fs = path.getFileSystem(conf);
			timer = new org.apache.hadoop.io.file.tfile.NanoTimer(false);
			rng = new java.util.Random(options.seed);
			keyLenGen = new org.apache.hadoop.io.file.tfile.RandomDistribution.Zipf(new java.util.Random
				(rng.nextLong()), options.minKeyLen, options.maxKeyLen, 1.2);
			org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG valLenGen = new org.apache.hadoop.io.file.tfile.RandomDistribution.Flat
				(new java.util.Random(rng.nextLong()), options.minValLength, options.maxValLength
				);
			org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG wordLenGen = new org.apache.hadoop.io.file.tfile.RandomDistribution.Flat
				(new java.util.Random(rng.nextLong()), options.minWordLen, options.maxWordLen);
			kvGen = new org.apache.hadoop.io.file.tfile.KVGenerator(rng, true, keyLenGen, valLenGen
				, wordLenGen, options.dictSize);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
			fs.delete(path, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.fs.FSDataOutputStream createFSOutput(org.apache.hadoop.fs.Path
			 name, org.apache.hadoop.fs.FileSystem fs)
		{
			if (fs.exists(name))
			{
				fs.delete(name, true);
			}
			org.apache.hadoop.fs.FSDataOutputStream fout = fs.create(name);
			return fout;
		}

		/// <exception cref="System.IO.IOException"/>
		private void createTFile()
		{
			long totalBytes = 0;
			org.apache.hadoop.fs.FSDataOutputStream fout = createFSOutput(path, fs);
			try
			{
				org.apache.hadoop.io.file.tfile.TFile.Writer writer = new org.apache.hadoop.io.file.tfile.TFile.Writer
					(fout, options.minBlockSize, options.compress, "memcmp", conf);
				try
				{
					org.apache.hadoop.io.BytesWritable key = new org.apache.hadoop.io.BytesWritable();
					org.apache.hadoop.io.BytesWritable val = new org.apache.hadoop.io.BytesWritable();
					timer.start();
					for (long i = 0; true; ++i)
					{
						if (i % 1000 == 0)
						{
							// test the size for every 1000 rows.
							if (fs.getFileStatus(path).getLen() >= options.fileSize)
							{
								break;
							}
						}
						kvGen.next(key, val, false);
						writer.append(key.get(), 0, key.getSize(), val.get(), 0, val.getSize());
						totalBytes += key.getSize();
						totalBytes += val.getSize();
					}
					timer.stop();
				}
				finally
				{
					writer.close();
				}
			}
			finally
			{
				fout.close();
			}
			double duration = (double)timer.read() / 1000;
			// in us.
			long fsize = fs.getFileStatus(path).getLen();
			System.Console.Out.printf("time: %s...uncompressed: %.2fMB...raw thrpt: %.2fMB/s\n"
				, timer.ToString(), (double)totalBytes / 1024 / 1024, totalBytes / duration);
			System.Console.Out.printf("time: %s...file size: %.2fMB...disk thrpt: %.2fMB/s\n"
				, timer.ToString(), (double)fsize / 1024 / 1024, fsize / duration);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void seekTFile()
		{
			int miss = 0;
			long totalBytes = 0;
			org.apache.hadoop.fs.FSDataInputStream fsdis = fs.open(path);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fsdis, fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.KeySampler kSampler = new org.apache.hadoop.io.file.tfile.KeySampler
				(rng, reader.getFirstKey(), reader.getLastKey(), keyLenGen);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			org.apache.hadoop.io.BytesWritable key = new org.apache.hadoop.io.BytesWritable();
			org.apache.hadoop.io.BytesWritable val = new org.apache.hadoop.io.BytesWritable();
			timer.reset();
			timer.start();
			for (int i = 0; i < options.seekCount; ++i)
			{
				kSampler.next(key);
				scanner.lowerBound(key.get(), 0, key.getSize());
				if (!scanner.atEnd())
				{
					scanner.entry().get(key, val);
					totalBytes += key.getSize();
					totalBytes += val.getSize();
				}
				else
				{
					++miss;
				}
			}
			timer.stop();
			double duration = (double)timer.read() / 1000;
			// in us.
			System.Console.Out.printf("time: %s...avg seek: %s...%d hit...%d miss...avg I/O size: %.2fKB\n"
				, timer.ToString(), org.apache.hadoop.io.file.tfile.NanoTimer.nanoTimeToString(timer
				.read() / options.seekCount), options.seekCount - miss, miss, (double)totalBytes
				 / 1024 / (options.seekCount - miss));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSeeks()
		{
			string[] supported = org.apache.hadoop.io.file.tfile.TFile.getSupportedCompressionAlgorithms
				();
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
			if (options.doCreate())
			{
				createTFile();
			}
			if (options.doRead())
			{
				seekTFile();
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

			/// <exception cref="org.apache.commons.cli.ParseException"/>
			public static org.apache.hadoop.io.file.tfile.TestTFileSeek.IntegerRange parse(string
				 s)
			{
				java.util.StringTokenizer st = new java.util.StringTokenizer(s, " \t,");
				if (st.countTokens() != 2)
				{
					throw new org.apache.commons.cli.ParseException("Bad integer specification: " + s
						);
				}
				int from = System.Convert.ToInt32(st.nextToken());
				int to = System.Convert.ToInt32(st.nextToken());
				return new org.apache.hadoop.io.file.tfile.TestTFileSeek.IntegerRange(from, to);
			}

			public virtual int from()
			{
				return from;
			}

			public virtual int to()
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

			internal string rootDir = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
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

			internal const int OP_CREATE = 1;

			internal const int OP_READ = 2;

			internal int op = OP_CREATE | OP_READ;

			internal bool proceed = false;

			public MyOptions(string[] args)
			{
				// hard coded constants
				seed = Sharpen.Runtime.nanoTime();
				try
				{
					org.apache.commons.cli.Options opts = buildOptions();
					org.apache.commons.cli.CommandLineParser parser = new org.apache.commons.cli.GnuParser
						();
					org.apache.commons.cli.CommandLine line = parser.parse(opts, args, true);
					processOptions(line, opts);
					validateOptions();
				}
				catch (org.apache.commons.cli.ParseException e)
				{
					System.Console.Out.WriteLine(e.Message);
					System.Console.Out.WriteLine("Try \"--help\" option for details.");
					setStopProceed();
				}
			}

			public virtual bool proceed()
			{
				return proceed;
			}

			private org.apache.commons.cli.Options buildOptions()
			{
				org.apache.commons.cli.Option compress = org.apache.commons.cli.OptionBuilder.create
					('c');
				org.apache.commons.cli.Option fileSize = org.apache.commons.cli.OptionBuilder.create
					('s');
				org.apache.commons.cli.Option fsInputBufferSz = org.apache.commons.cli.OptionBuilder
					.create('i');
				org.apache.commons.cli.Option fsOutputBufferSize = org.apache.commons.cli.OptionBuilder
					.create('o');
				org.apache.commons.cli.Option keyLen = org.apache.commons.cli.OptionBuilder.create
					('k');
				org.apache.commons.cli.Option valueLen = org.apache.commons.cli.OptionBuilder.create
					('v');
				org.apache.commons.cli.Option blockSz = org.apache.commons.cli.OptionBuilder.create
					('b');
				org.apache.commons.cli.Option seed = org.apache.commons.cli.OptionBuilder.create(
					'S');
				org.apache.commons.cli.Option operation = org.apache.commons.cli.OptionBuilder.create
					('x');
				org.apache.commons.cli.Option rootDir = org.apache.commons.cli.OptionBuilder.create
					('r');
				org.apache.commons.cli.Option file = org.apache.commons.cli.OptionBuilder.create(
					'f');
				org.apache.commons.cli.Option seekCount = org.apache.commons.cli.OptionBuilder.create
					('n');
				org.apache.commons.cli.Option help = org.apache.commons.cli.OptionBuilder.create(
					"h");
				return new org.apache.commons.cli.Options().addOption(compress).addOption(fileSize
					).addOption(fsInputBufferSz).addOption(fsOutputBufferSize).addOption(keyLen).addOption
					(blockSz).addOption(rootDir).addOption(valueLen).addOption(operation).addOption(
					seekCount).addOption(file).addOption(help);
			}

			/// <exception cref="org.apache.commons.cli.ParseException"/>
			private void processOptions(org.apache.commons.cli.CommandLine line, org.apache.commons.cli.Options
				 opts)
			{
				// --help -h and --version -V must be processed first.
				if (line.hasOption('h'))
				{
					org.apache.commons.cli.HelpFormatter formatter = new org.apache.commons.cli.HelpFormatter
						();
					System.Console.Out.WriteLine("TFile and SeqFile benchmark.");
					System.Console.Out.WriteLine();
					formatter.printHelp(100, "java ... TestTFileSeqFileComparison [options]", "\nSupported options:"
						, opts, string.Empty);
					return;
				}
				if (line.hasOption('c'))
				{
					compress = line.getOptionValue('c');
				}
				if (line.hasOption('d'))
				{
					dictSize = System.Convert.ToInt32(line.getOptionValue('d'));
				}
				if (line.hasOption('s'))
				{
					fileSize = long.Parse(line.getOptionValue('s')) * 1024 * 1024;
				}
				if (line.hasOption('i'))
				{
					fsInputBufferSize = System.Convert.ToInt32(line.getOptionValue('i'));
				}
				if (line.hasOption('o'))
				{
					fsOutputBufferSize = System.Convert.ToInt32(line.getOptionValue('o'));
				}
				if (line.hasOption('n'))
				{
					seekCount = System.Convert.ToInt32(line.getOptionValue('n'));
				}
				if (line.hasOption('k'))
				{
					org.apache.hadoop.io.file.tfile.TestTFileSeek.IntegerRange ir = org.apache.hadoop.io.file.tfile.TestTFileSeek.IntegerRange
						.parse(line.getOptionValue('k'));
					minKeyLen = ir.from();
					maxKeyLen = ir.to();
				}
				if (line.hasOption('v'))
				{
					org.apache.hadoop.io.file.tfile.TestTFileSeek.IntegerRange ir = org.apache.hadoop.io.file.tfile.TestTFileSeek.IntegerRange
						.parse(line.getOptionValue('v'));
					minValLength = ir.from();
					maxValLength = ir.to();
				}
				if (line.hasOption('b'))
				{
					minBlockSize = System.Convert.ToInt32(line.getOptionValue('b')) * 1024;
				}
				if (line.hasOption('r'))
				{
					rootDir = line.getOptionValue('r');
				}
				if (line.hasOption('f'))
				{
					file = line.getOptionValue('f');
				}
				if (line.hasOption('S'))
				{
					seed = long.Parse(line.getOptionValue('S'));
				}
				if (line.hasOption('x'))
				{
					string strOp = line.getOptionValue('x');
					if (strOp.Equals("r"))
					{
						op = OP_READ;
					}
					else
					{
						if (strOp.Equals("w"))
						{
							op = OP_CREATE;
						}
						else
						{
							if (strOp.Equals("rw"))
							{
								op = OP_CREATE | OP_READ;
							}
							else
							{
								throw new org.apache.commons.cli.ParseException("Unknown action specifier: " + strOp
									);
							}
						}
					}
				}
				proceed = true;
			}

			/// <exception cref="org.apache.commons.cli.ParseException"/>
			private void validateOptions()
			{
				if (!compress.Equals("none") && !compress.Equals("lzo") && !compress.Equals("gz"))
				{
					throw new org.apache.commons.cli.ParseException("Unknown compression scheme: " + 
						compress);
				}
				if (minKeyLen >= maxKeyLen)
				{
					throw new org.apache.commons.cli.ParseException("Max key length must be greater than min key length."
						);
				}
				if (minValLength >= maxValLength)
				{
					throw new org.apache.commons.cli.ParseException("Max value length must be greater than min value length."
						);
				}
				if (minWordLen >= maxWordLen)
				{
					throw new org.apache.commons.cli.ParseException("Max word length must be greater than min word length."
						);
				}
				return;
			}

			private void setStopProceed()
			{
				proceed = false;
			}

			public virtual bool doCreate()
			{
				return (op & OP_CREATE) != 0;
			}

			public virtual bool doRead()
			{
				return (op & OP_READ) != 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] argv)
		{
			org.apache.hadoop.io.file.tfile.TestTFileSeek testCase = new org.apache.hadoop.io.file.tfile.TestTFileSeek
				();
			org.apache.hadoop.io.file.tfile.TestTFileSeek.MyOptions options = new org.apache.hadoop.io.file.tfile.TestTFileSeek.MyOptions
				(argv);
			if (options.proceed == false)
			{
				return;
			}
			testCase.options = options;
			testCase.setUp();
			testCase.testSeeks();
			testCase.tearDown();
		}
	}
}
