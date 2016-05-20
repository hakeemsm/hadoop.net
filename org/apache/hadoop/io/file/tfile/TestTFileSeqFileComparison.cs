using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestTFileSeqFileComparison : NUnit.Framework.TestCase
	{
		internal org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.MyOptions options;

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.conf.Configuration conf;

		private long startTimeEpoch;

		private long finishTimeEpoch;

		private java.text.DateFormat formatter;

		internal byte[][] dictionary;

		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			if (options == null)
			{
				options = new org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.MyOptions
					(new string[0]);
			}
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSize);
			conf.setInt("tfile.fs.output.buffer.size", options.fsOutputBufferSize);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(options.rootDir);
			fs = path.getFileSystem(conf);
			formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			setUpDictionary();
		}

		private void setUpDictionary()
		{
			java.util.Random rng = new java.util.Random();
			dictionary = new byte[options.dictSize][];
			for (int i = 0; i < options.dictSize; ++i)
			{
				int len = rng.nextInt(options.maxWordLen - options.minWordLen) + options.minWordLen;
				dictionary[i] = new byte[len];
				rng.nextBytes(dictionary[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
		}

		// do nothing
		/// <exception cref="System.IO.IOException"/>
		public virtual void startTime()
		{
			startTimeEpoch = org.apache.hadoop.util.Time.now();
			System.Console.Out.WriteLine(formatTime() + " Started timing.");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void stopTime()
		{
			finishTimeEpoch = org.apache.hadoop.util.Time.now();
			System.Console.Out.WriteLine(formatTime() + " Stopped timing.");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long getIntervalMillis()
		{
			return finishTimeEpoch - startTimeEpoch;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void printlnWithTimestamp(string message)
		{
			System.Console.Out.WriteLine(formatTime() + "  " + message);
		}

		/*
		* Format millis into minutes and seconds.
		*/
		public virtual string formatTime(long milis)
		{
			return formatter.format(milis);
		}

		public virtual string formatTime()
		{
			return formatTime(org.apache.hadoop.util.Time.now());
		}

		private interface KVAppendable
		{
			/// <exception cref="System.IO.IOException"/>
			void append(org.apache.hadoop.io.BytesWritable key, org.apache.hadoop.io.BytesWritable
				 value);

			/// <exception cref="System.IO.IOException"/>
			void close();
		}

		private interface KVReadable
		{
			byte[] getKey();

			byte[] getValue();

			int getKeyLength();

			int getValueLength();

			/// <exception cref="System.IO.IOException"/>
			bool next();

			/// <exception cref="System.IO.IOException"/>
			void close();
		}

		internal class TFileAppendable : org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVAppendable
		{
			private org.apache.hadoop.fs.FSDataOutputStream fsdos;

			private org.apache.hadoop.io.file.tfile.TFile.Writer writer;

			/// <exception cref="System.IO.IOException"/>
			public TFileAppendable(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
				 path, string compress, int minBlkSize, int osBufferSize, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.fsdos = fs.create(path, true, osBufferSize);
				this.writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(fsdos, minBlkSize, 
					compress, null, conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void append(org.apache.hadoop.io.BytesWritable key, org.apache.hadoop.io.BytesWritable
				 value)
			{
				writer.append(key.get(), 0, key.getSize(), value.get(), 0, value.getSize());
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				writer.close();
				fsdos.close();
			}
		}

		internal class TFileReadable : org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVReadable
		{
			private org.apache.hadoop.fs.FSDataInputStream fsdis;

			private org.apache.hadoop.io.file.tfile.TFile.Reader reader;

			private org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner;

			private byte[] keyBuffer;

			private int keyLength;

			private byte[] valueBuffer;

			private int valueLength;

			/// <exception cref="System.IO.IOException"/>
			public TFileReadable(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
				 path, int osBufferSize, org.apache.hadoop.conf.Configuration conf)
			{
				this.fsdis = fs.open(path, osBufferSize);
				this.reader = new org.apache.hadoop.io.file.tfile.TFile.Reader(fsdis, fs.getFileStatus
					(path).getLen(), conf);
				this.scanner = reader.createScanner();
				keyBuffer = new byte[32];
				valueBuffer = new byte[32];
			}

			private void checkKeyBuffer(int size)
			{
				if (size <= keyBuffer.Length)
				{
					return;
				}
				keyBuffer = new byte[System.Math.max(2 * keyBuffer.Length, 2 * size - keyBuffer.Length
					)];
			}

			private void checkValueBuffer(int size)
			{
				if (size <= valueBuffer.Length)
				{
					return;
				}
				valueBuffer = new byte[System.Math.max(2 * valueBuffer.Length, 2 * size - valueBuffer
					.Length)];
			}

			public virtual byte[] getKey()
			{
				return keyBuffer;
			}

			public virtual int getKeyLength()
			{
				return keyLength;
			}

			public virtual byte[] getValue()
			{
				return valueBuffer;
			}

			public virtual int getValueLength()
			{
				return valueLength;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool next()
			{
				if (scanner.atEnd())
				{
					return false;
				}
				org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry entry = scanner.entry(
					);
				keyLength = entry.getKeyLength();
				checkKeyBuffer(keyLength);
				entry.getKey(keyBuffer);
				valueLength = entry.getValueLength();
				checkValueBuffer(valueLength);
				entry.getValue(valueBuffer);
				scanner.advance();
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				scanner.close();
				reader.close();
				fsdis.close();
			}
		}

		internal class SeqFileAppendable : org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVAppendable
		{
			private org.apache.hadoop.fs.FSDataOutputStream fsdos;

			private org.apache.hadoop.io.SequenceFile.Writer writer;

			/// <exception cref="System.IO.IOException"/>
			public SeqFileAppendable(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
				 path, int osBufferSize, string compress, int minBlkSize)
			{
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
					();
				conf.setBoolean("hadoop.native.lib", true);
				org.apache.hadoop.io.compress.CompressionCodec codec = null;
				if ("lzo".Equals(compress))
				{
					codec = org.apache.hadoop.io.file.tfile.Compression.Algorithm.LZO.getCodec();
				}
				else
				{
					if ("gz".Equals(compress))
					{
						codec = org.apache.hadoop.io.file.tfile.Compression.Algorithm.GZ.getCodec();
					}
					else
					{
						if (!"none".Equals(compress))
						{
							throw new System.IO.IOException("Codec not supported.");
						}
					}
				}
				this.fsdos = fs.create(path, true, osBufferSize);
				if (!"none".Equals(compress))
				{
					writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, fsdos, Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.io.BytesWritable)), Sharpen.Runtime.getClassForType(typeof(
						org.apache.hadoop.io.BytesWritable)), org.apache.hadoop.io.SequenceFile.CompressionType
						.BLOCK, codec);
				}
				else
				{
					writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, fsdos, Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.io.BytesWritable)), Sharpen.Runtime.getClassForType(typeof(
						org.apache.hadoop.io.BytesWritable)), org.apache.hadoop.io.SequenceFile.CompressionType
						.NONE, null);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void append(org.apache.hadoop.io.BytesWritable key, org.apache.hadoop.io.BytesWritable
				 value)
			{
				writer.append(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				writer.close();
				fsdos.close();
			}
		}

		internal class SeqFileReadable : org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVReadable
		{
			private org.apache.hadoop.io.SequenceFile.Reader reader;

			private org.apache.hadoop.io.BytesWritable key;

			private org.apache.hadoop.io.BytesWritable value;

			/// <exception cref="System.IO.IOException"/>
			public SeqFileReadable(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
				 path, int osBufferSize)
			{
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
					();
				conf.setInt("io.file.buffer.size", osBufferSize);
				reader = new org.apache.hadoop.io.SequenceFile.Reader(fs, path, conf);
				key = new org.apache.hadoop.io.BytesWritable();
				value = new org.apache.hadoop.io.BytesWritable();
			}

			public virtual byte[] getKey()
			{
				return key.get();
			}

			public virtual int getKeyLength()
			{
				return key.getSize();
			}

			public virtual byte[] getValue()
			{
				return value.get();
			}

			public virtual int getValueLength()
			{
				return value.getSize();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool next()
			{
				return reader.next(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				reader.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void reportStats(org.apache.hadoop.fs.Path path, long totalBytes)
		{
			long duration = getIntervalMillis();
			long fsize = fs.getFileStatus(path).getLen();
			printlnWithTimestamp(string.format("Duration: %dms...total size: %.2fMB...raw thrpt: %.2fMB/s"
				, duration, (double)totalBytes / 1024 / 1024, (double)totalBytes / duration * 1000
				 / 1024 / 1024));
			printlnWithTimestamp(string.format("Compressed size: %.2fMB...compressed thrpt: %.2fMB/s."
				, (double)fsize / 1024 / 1024, (double)fsize / duration * 1000 / 1024 / 1024));
		}

		private void fillBuffer(java.util.Random rng, org.apache.hadoop.io.BytesWritable 
			bw, byte[] tmp, int len)
		{
			int n = 0;
			while (n < len)
			{
				byte[] word = dictionary[rng.nextInt(dictionary.Length)];
				int l = System.Math.min(word.Length, len - n);
				System.Array.Copy(word, 0, tmp, n, l);
				n += l;
			}
			bw.set(tmp, 0, len);
		}

		/// <exception cref="System.IO.IOException"/>
		private void timeWrite(org.apache.hadoop.fs.Path path, org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVAppendable
			 appendable, int baseKlen, int baseVlen, long fileSize)
		{
			int maxKlen = baseKlen * 2;
			int maxVlen = baseVlen * 2;
			org.apache.hadoop.io.BytesWritable key = new org.apache.hadoop.io.BytesWritable();
			org.apache.hadoop.io.BytesWritable value = new org.apache.hadoop.io.BytesWritable
				();
			byte[] keyBuffer = new byte[maxKlen];
			byte[] valueBuffer = new byte[maxVlen];
			java.util.Random rng = new java.util.Random(options.seed);
			long totalBytes = 0;
			printlnWithTimestamp("Start writing: " + path.getName() + "...");
			startTime();
			for (long i = 0; true; ++i)
			{
				if (i % 1000 == 0)
				{
					// test the size for every 1000 rows.
					if (fs.getFileStatus(path).getLen() >= fileSize)
					{
						break;
					}
				}
				int klen = rng.nextInt(baseKlen) + baseKlen;
				int vlen = rng.nextInt(baseVlen) + baseVlen;
				fillBuffer(rng, key, keyBuffer, klen);
				fillBuffer(rng, value, valueBuffer, vlen);
				key.set(keyBuffer, 0, klen);
				value.set(valueBuffer, 0, vlen);
				appendable.append(key, value);
				totalBytes += klen;
				totalBytes += vlen;
			}
			stopTime();
			appendable.close();
			reportStats(path, totalBytes);
		}

		/// <exception cref="System.IO.IOException"/>
		private void timeRead(org.apache.hadoop.fs.Path path, org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVReadable
			 readable)
		{
			printlnWithTimestamp("Start reading: " + path.getName() + "...");
			long totalBytes = 0;
			startTime();
			for (; readable.next(); )
			{
				totalBytes += readable.getKeyLength();
				totalBytes += readable.getValueLength();
			}
			stopTime();
			readable.close();
			reportStats(path, totalBytes);
		}

		/// <exception cref="System.IO.IOException"/>
		private void createTFile(string parameters, string compress)
		{
			System.Console.Out.WriteLine("=== TFile: Creation (" + parameters + ") === ");
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(options.rootDir, "TFile.Performance"
				);
			org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVAppendable appendable
				 = new org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.TFileAppendable
				(fs, path, compress, options.minBlockSize, options.osOutputBufferSize, conf);
			timeWrite(path, appendable, options.keyLength, options.valueLength, options.fileSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void readTFile(string parameters, bool delFile)
		{
			System.Console.Out.WriteLine("=== TFile: Reading (" + parameters + ") === ");
			{
				org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(options.rootDir, "TFile.Performance"
					);
				org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVReadable readable = 
					new org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.TFileReadable(fs, 
					path, options.osInputBufferSize, conf);
				timeRead(path, readable);
				if (delFile)
				{
					if (fs.exists(path))
					{
						fs.delete(path, true);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void createSeqFile(string parameters, string compress)
		{
			System.Console.Out.WriteLine("=== SeqFile: Creation (" + parameters + ") === ");
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(options.rootDir, "SeqFile.Performance"
				);
			org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVAppendable appendable
				 = new org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.SeqFileAppendable
				(fs, path, options.osOutputBufferSize, compress, options.minBlockSize);
			timeWrite(path, appendable, options.keyLength, options.valueLength, options.fileSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void readSeqFile(string parameters, bool delFile)
		{
			System.Console.Out.WriteLine("=== SeqFile: Reading (" + parameters + ") === ");
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(options.rootDir, "SeqFile.Performance"
				);
			org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.KVReadable readable = 
				new org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.SeqFileReadable(fs
				, path, options.osInputBufferSize);
			timeRead(path, readable);
			if (delFile)
			{
				if (fs.exists(path))
				{
					fs.delete(path, true);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void compareRun(string compress)
		{
			string[] supported = org.apache.hadoop.io.file.tfile.TFile.getSupportedCompressionAlgorithms
				();
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
			string parameters = parameters2String(options);
			createSeqFile(parameters, compress);
			readSeqFile(parameters, true);
			createTFile(parameters, compress);
			readTFile(parameters, true);
			createTFile(parameters, compress);
			readTFile(parameters, true);
			createSeqFile(parameters, compress);
			readSeqFile(parameters, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRunComparisons()
		{
			string[] compresses = new string[] { "none", "lzo", "gz" };
			foreach (string compress in compresses)
			{
				if (compress.Equals("none"))
				{
					conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeNone);
					conf.setInt("tfile.fs.output.buffer.size", options.fsOutputBufferSizeNone);
				}
				else
				{
					if (compress.Equals("lzo"))
					{
						conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeLzo);
						conf.setInt("tfile.fs.output.buffer.size", options.fsOutputBufferSizeLzo);
					}
					else
					{
						conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSizeGz);
						conf.setInt("tfile.fs.output.buffer.size", options.fsOutputBufferSizeGz);
					}
				}
				compareRun(compress);
			}
		}

		private static string parameters2String(org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.MyOptions
			 options)
		{
			return string.format("KLEN: %d-%d... VLEN: %d-%d...MinBlkSize: %.2fKB...Target Size: %.2fMB...Compression: ...%s"
				, options.keyLength, options.keyLength * 2, options.valueLength, options.valueLength
				 * 2, (double)options.minBlockSize / 1024, (double)options.fileSize / 1024 / 1024
				, options.compress);
		}

		private class MyOptions
		{
			internal string rootDir = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
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

			internal const int OP_CREATE = 1;

			internal const int OP_READ = 2;

			internal int op = OP_READ;

			internal bool proceed = false;

			public MyOptions(string[] args)
			{
				// special variable only for unit testing.
				// un-exposed parameters.
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
				org.apache.commons.cli.Option ditSize = org.apache.commons.cli.OptionBuilder.create
					('d');
				org.apache.commons.cli.Option fileSize = org.apache.commons.cli.OptionBuilder.create
					('s');
				org.apache.commons.cli.Option format = org.apache.commons.cli.OptionBuilder.create
					('f');
				org.apache.commons.cli.Option fsInputBufferSz = org.apache.commons.cli.OptionBuilder
					.create('i');
				org.apache.commons.cli.Option fsOutputBufferSize = org.apache.commons.cli.OptionBuilder
					.create('o');
				org.apache.commons.cli.Option keyLen = org.apache.commons.cli.OptionBuilder.create
					('k');
				org.apache.commons.cli.Option valueLen = org.apache.commons.cli.OptionBuilder.create
					('v');
				org.apache.commons.cli.Option wordLen = org.apache.commons.cli.OptionBuilder.create
					('w');
				org.apache.commons.cli.Option blockSz = org.apache.commons.cli.OptionBuilder.create
					('b');
				org.apache.commons.cli.Option seed = org.apache.commons.cli.OptionBuilder.create(
					'S');
				org.apache.commons.cli.Option operation = org.apache.commons.cli.OptionBuilder.create
					('x');
				org.apache.commons.cli.Option rootDir = org.apache.commons.cli.OptionBuilder.create
					('r');
				org.apache.commons.cli.Option help = org.apache.commons.cli.OptionBuilder.create(
					"h");
				return new org.apache.commons.cli.Options().addOption(compress).addOption(ditSize
					).addOption(fileSize).addOption(format).addOption(fsInputBufferSz).addOption(fsOutputBufferSize
					).addOption(keyLen).addOption(wordLen).addOption(blockSz).addOption(rootDir).addOption
					(valueLen).addOption(operation).addOption(help);
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
				if (line.hasOption('f'))
				{
					format = line.getOptionValue('f');
				}
				if (line.hasOption('i'))
				{
					fsInputBufferSize = System.Convert.ToInt32(line.getOptionValue('i'));
				}
				if (line.hasOption('o'))
				{
					fsOutputBufferSize = System.Convert.ToInt32(line.getOptionValue('o'));
				}
				if (line.hasOption('k'))
				{
					keyLength = System.Convert.ToInt32(line.getOptionValue('k'));
				}
				if (line.hasOption('v'))
				{
					valueLength = System.Convert.ToInt32(line.getOptionValue('v'));
				}
				if (line.hasOption('b'))
				{
					minBlockSize = System.Convert.ToInt32(line.getOptionValue('b')) * 1024;
				}
				if (line.hasOption('r'))
				{
					rootDir = line.getOptionValue('r');
				}
				if (line.hasOption('S'))
				{
					seed = long.Parse(line.getOptionValue('S'));
				}
				if (line.hasOption('w'))
				{
					string min_max = line.getOptionValue('w');
					java.util.StringTokenizer st = new java.util.StringTokenizer(min_max, " \t,");
					if (st.countTokens() != 2)
					{
						throw new org.apache.commons.cli.ParseException("Bad word length specification: "
							 + min_max);
					}
					minWordLen = System.Convert.ToInt32(st.nextToken());
					maxWordLen = System.Convert.ToInt32(st.nextToken());
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
				if (!format.Equals("tfile") && !format.Equals("seqfile"))
				{
					throw new org.apache.commons.cli.ParseException("Unknown file format: " + format);
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
		public static void Main(string[] args)
		{
			org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison testCase = new org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison
				();
			org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.MyOptions options = new 
				org.apache.hadoop.io.file.tfile.TestTFileSeqFileComparison.MyOptions(args);
			if (options.proceed == false)
			{
				return;
			}
			testCase.options = options;
			string parameters = parameters2String(options);
			testCase.setUp();
			if (testCase.options.format.Equals("tfile"))
			{
				if (options.doCreate())
				{
					testCase.createTFile(parameters, options.compress);
				}
				if (options.doRead())
				{
					testCase.readTFile(parameters, options.doCreate());
				}
			}
			else
			{
				if (options.doCreate())
				{
					testCase.createSeqFile(parameters, options.compress);
				}
				if (options.doRead())
				{
					testCase.readSeqFile(parameters, options.doCreate());
				}
			}
			testCase.tearDown();
		}
	}
}
