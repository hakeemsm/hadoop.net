using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using NUnit.Framework;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress.Bzip2;
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Compress
{
	public class TestCodec
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCodec));

		private Configuration conf = new Configuration();

		private int count = 10000;

		private int seed = new Random().Next();

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDefaultCodec()
		{
			CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DefaultCodec");
			CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.DefaultCodec");
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGzipCodec()
		{
			CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
			CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBZip2Codec()
		{
			Configuration conf = new Configuration();
			conf.Set("io.compression.codec.bzip2.library", "java-builtin");
			CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
			CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBZip2NativeCodec()
		{
			Configuration conf = new Configuration();
			conf.Set("io.compression.codec.bzip2.library", "system-native");
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				if (Bzip2Factory.IsNativeBzip2Loaded(conf))
				{
					CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
					CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
					conf.Set("io.compression.codec.bzip2.library", "java-builtin");
					CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
					CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
				}
				else
				{
					Log.Warn("Native hadoop library available but native bzip2 is not");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestSnappyCodec()
		{
			if (SnappyCodec.IsNativeCodeLoaded())
			{
				CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.SnappyCodec");
				CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.SnappyCodec");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestLz4Codec()
		{
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				if (Lz4Codec.IsNativeCodeLoaded())
				{
					conf.SetBoolean(CommonConfigurationKeys.IoCompressionCodecLz4Uselz4hcKey, false);
					CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
					CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.Lz4Codec");
					conf.SetBoolean(CommonConfigurationKeys.IoCompressionCodecLz4Uselz4hcKey, true);
					CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
					CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.Lz4Codec");
				}
				else
				{
					NUnit.Framework.Assert.Fail("Native hadoop library available but lz4 not");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDeflateCodec()
		{
			CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DeflateCodec");
			CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.DeflateCodec");
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGzipCodecWithParam()
		{
			Configuration conf = new Configuration(this.conf);
			ZlibFactory.SetCompressionLevel(conf, ZlibCompressor.CompressionLevel.BestCompression
				);
			ZlibFactory.SetCompressionStrategy(conf, ZlibCompressor.CompressionStrategy.HuffmanOnly
				);
			CodecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
			CodecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CodecTest(Configuration conf, int seed, int count, string codecClass
			)
		{
			// Create the codec
			CompressionCodec codec = null;
			try
			{
				codec = (CompressionCodec)ReflectionUtils.NewInstance(conf.GetClassByName(codecClass
					), conf);
			}
			catch (TypeLoadException)
			{
				throw new IOException("Illegal codec!");
			}
			Log.Info("Created a Codec object of type: " + codecClass);
			// Generate data
			DataOutputBuffer data = new DataOutputBuffer();
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			for (int i = 0; i < count; ++i)
			{
				generator.Next();
				RandomDatum key = generator.GetKey();
				RandomDatum value = generator.GetValue();
				key.Write(data);
				value.Write(data);
			}
			Log.Info("Generated " + count + " records");
			// Compress data
			DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
			CompressionOutputStream deflateFilter = codec.CreateOutputStream(compressedDataBuffer
				);
			DataOutputStream deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter
				));
			deflateOut.Write(data.GetData(), 0, data.GetLength());
			deflateOut.Flush();
			deflateFilter.Finish();
			Log.Info("Finished compressing data");
			// De-compress data
			DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
			deCompressedDataBuffer.Reset(compressedDataBuffer.GetData(), 0, compressedDataBuffer
				.GetLength());
			CompressionInputStream inflateFilter = codec.CreateInputStream(deCompressedDataBuffer
				);
			DataInputStream inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter
				));
			// Check
			DataInputBuffer originalData = new DataInputBuffer();
			originalData.Reset(data.GetData(), 0, data.GetLength());
			DataInputStream originalIn = new DataInputStream(new BufferedInputStream(originalData
				));
			for (int i_1 = 0; i_1 < count; ++i_1)
			{
				RandomDatum k1 = new RandomDatum();
				RandomDatum v1 = new RandomDatum();
				k1.ReadFields(originalIn);
				v1.ReadFields(originalIn);
				RandomDatum k2 = new RandomDatum();
				RandomDatum v2 = new RandomDatum();
				k2.ReadFields(inflateIn);
				v2.ReadFields(inflateIn);
				Assert.True("original and compressed-then-decompressed-output not equal"
					, k1.Equals(k2) && v1.Equals(v2));
				// original and compressed-then-decompressed-output have the same hashCode
				IDictionary<RandomDatum, string> m = new Dictionary<RandomDatum, string>();
				m[k1] = k1.ToString();
				m[v1] = v1.ToString();
				string result = m[k2];
				Assert.Equal("k1 and k2 hashcode not equal", result, k1.ToString
					());
				result = m[v2];
				Assert.Equal("v1 and v2 hashcode not equal", result, v1.ToString
					());
			}
			// De-compress data byte-at-a-time
			originalData.Reset(data.GetData(), 0, data.GetLength());
			deCompressedDataBuffer.Reset(compressedDataBuffer.GetData(), 0, compressedDataBuffer
				.GetLength());
			inflateFilter = codec.CreateInputStream(deCompressedDataBuffer);
			// Check
			originalIn = new DataInputStream(new BufferedInputStream(originalData));
			int expected;
			do
			{
				expected = originalIn.Read();
				Assert.Equal("Inflated stream read by byte does not match", expected
					, inflateFilter.Read());
			}
			while (expected != -1);
			Log.Info("SUCCESS! Completed checking " + count + " records");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSplitableCodecs()
		{
			TestSplitableCodec(typeof(BZip2Codec));
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestSplitableCodec(Type codecClass)
		{
			long Deflbytes = 2 * 1024 * 1024;
			Configuration conf = new Configuration();
			Random rand = new Random();
			long seed = rand.NextLong();
			Log.Info("seed: " + seed);
			rand.SetSeed(seed);
			SplittableCompressionCodec codec = ReflectionUtils.NewInstance(codecClass, conf);
			FileSystem fs = FileSystem.GetLocal(conf);
			FileStatus infile = fs.GetFileStatus(WriteSplitTestFile(fs, rand, codec, Deflbytes
				));
			if (infile.GetLen() > int.MaxValue)
			{
				NUnit.Framework.Assert.Fail("Unexpected compression: " + Deflbytes + " -> " + infile
					.GetLen());
			}
			int flen = (int)infile.GetLen();
			Text line = new Text();
			Decompressor dcmp = CodecPool.GetDecompressor(codec);
			try
			{
				for (int pos = 0; pos < infile.GetLen(); pos += rand.Next(flen / 8))
				{
					// read from random positions, verifying that there exist two sequential
					// lines as written in writeSplitTestFile
					SplitCompressionInputStream @in = codec.CreateInputStream(fs.Open(infile.GetPath(
						)), dcmp, pos, flen, SplittableCompressionCodec.READ_MODE.Byblock);
					if (@in.GetAdjustedStart() >= flen)
					{
						break;
					}
					Log.Info("SAMPLE " + @in.GetAdjustedStart() + "," + @in.GetAdjustedEnd());
					LineReader lreader = new LineReader(@in);
					lreader.ReadLine(line);
					// ignore; likely partial
					if (@in.GetPos() >= flen)
					{
						break;
					}
					lreader.ReadLine(line);
					int seq1 = ReadLeadingInt(line);
					lreader.ReadLine(line);
					if (@in.GetPos() >= flen)
					{
						break;
					}
					int seq2 = ReadLeadingInt(line);
					Assert.Equal("Mismatched lines", seq1 + 1, seq2);
				}
			}
			finally
			{
				CodecPool.ReturnDecompressor(dcmp);
			}
			// remove on success
			fs.Delete(infile.GetPath().GetParent(), true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static int ReadLeadingInt(Text txt)
		{
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(txt.GetBytes()
				));
			return @in.ReadInt();
		}

		/// <summary>Write infLen bytes (deflated) to file in test dir using codec.</summary>
		/// <remarks>
		/// Write infLen bytes (deflated) to file in test dir using codec.
		/// Records are of the form
		/// &lt;i&gt;&lt;b64 rand&gt;&lt;i+i&gt;&lt;b64 rand&gt;
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static Path WriteSplitTestFile(FileSystem fs, Random rand, CompressionCodec
			 codec, long infLen)
		{
			int RecSize = 1024;
			Path wd = new Path(new Path(Runtime.GetProperty("test.build.data", "/tmp")).MakeQualified
				(fs), codec.GetType().Name);
			Path file = new Path(wd, "test" + codec.GetDefaultExtension());
			byte[] b = new byte[RecSize];
			Base64 b64 = new Base64(0, null);
			DataOutputStream fout = null;
			Compressor cmp = CodecPool.GetCompressor(codec);
			try
			{
				fout = new DataOutputStream(codec.CreateOutputStream(fs.Create(file, true), cmp));
				DataOutputBuffer dob = new DataOutputBuffer(RecSize * 4 / 3 + 4);
				int seq = 0;
				while (infLen > 0)
				{
					rand.NextBytes(b);
					byte[] b64enc = b64.Encode(b);
					// ensures rand printable, no LF
					dob.Reset();
					dob.WriteInt(seq);
					System.Array.Copy(dob.GetData(), 0, b64enc, 0, dob.GetLength());
					fout.Write(b64enc);
					fout.Write('\n');
					++seq;
					infLen -= b64enc.Length;
				}
				Log.Info("Wrote " + seq + " records to " + file);
			}
			finally
			{
				IOUtils.Cleanup(Log, fout);
				CodecPool.ReturnCompressor(cmp);
			}
			return file;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCodecPoolGzipReuse()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, true);
			if (!ZlibFactory.IsNativeZlibLoaded(conf))
			{
				Log.Warn("testCodecPoolGzipReuse skipped: native libs not loaded");
				return;
			}
			GzipCodec gzc = ReflectionUtils.NewInstance<GzipCodec>(conf);
			DefaultCodec dfc = ReflectionUtils.NewInstance<DefaultCodec>(conf);
			Compressor c1 = CodecPool.GetCompressor(gzc);
			Compressor c2 = CodecPool.GetCompressor(dfc);
			CodecPool.ReturnCompressor(c1);
			CodecPool.ReturnCompressor(c2);
			Assert.True("Got mismatched ZlibCompressor", c2 != CodecPool.GetCompressor
				(gzc));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void GzipReinitTest(Configuration conf, CompressionCodec codec)
		{
			// Add codec to cache
			ZlibFactory.SetCompressionLevel(conf, ZlibCompressor.CompressionLevel.BestCompression
				);
			ZlibFactory.SetCompressionStrategy(conf, ZlibCompressor.CompressionStrategy.DefaultStrategy
				);
			Compressor c1 = CodecPool.GetCompressor(codec);
			CodecPool.ReturnCompressor(c1);
			// reset compressor's compression level to perform no compression
			ZlibFactory.SetCompressionLevel(conf, ZlibCompressor.CompressionLevel.NoCompression
				);
			Compressor c2 = CodecPool.GetCompressor(codec, conf);
			// ensure same compressor placed earlier
			Assert.True("Got mismatched ZlibCompressor", c1 == c2);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			CompressionOutputStream cos = null;
			// write trivially compressable data
			byte[] b = new byte[1 << 15];
			Arrays.Fill(b, unchecked((byte)43));
			try
			{
				cos = codec.CreateOutputStream(bos, c2);
				cos.Write(b);
			}
			finally
			{
				if (cos != null)
				{
					cos.Close();
				}
				CodecPool.ReturnCompressor(c2);
			}
			byte[] outbytes = bos.ToByteArray();
			// verify data were not compressed
			Assert.True("Compressed bytes contrary to configuration", outbytes
				.Length >= b.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CodecTestWithNOCompression(Configuration conf, string codecClass
			)
		{
			// Create a compressor with NO_COMPRESSION and make sure that
			// output is not compressed by comparing the size with the
			// original input
			CompressionCodec codec = null;
			ZlibFactory.SetCompressionLevel(conf, ZlibCompressor.CompressionLevel.NoCompression
				);
			try
			{
				codec = (CompressionCodec)ReflectionUtils.NewInstance(conf.GetClassByName(codecClass
					), conf);
			}
			catch (TypeLoadException)
			{
				throw new IOException("Illegal codec!");
			}
			Compressor c = codec.CreateCompressor();
			// ensure same compressor placed earlier
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			CompressionOutputStream cos = null;
			// write trivially compressable data
			byte[] b = new byte[1 << 15];
			Arrays.Fill(b, unchecked((byte)43));
			try
			{
				cos = codec.CreateOutputStream(bos, c);
				cos.Write(b);
			}
			finally
			{
				if (cos != null)
				{
					cos.Close();
				}
			}
			byte[] outbytes = bos.ToByteArray();
			// verify data were not compressed
			Assert.True("Compressed bytes contrary to configuration(NO_COMPRESSION)"
				, outbytes.Length >= b.Length);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCodecInitWithCompressionLevel()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, true);
			if (ZlibFactory.IsNativeZlibLoaded(conf))
			{
				Log.Info("testCodecInitWithCompressionLevel with native");
				CodecTestWithNOCompression(conf, "org.apache.hadoop.io.compress.GzipCodec");
				CodecTestWithNOCompression(conf, "org.apache.hadoop.io.compress.DefaultCodec");
			}
			else
			{
				Log.Warn("testCodecInitWithCompressionLevel for native skipped" + ": native libs not loaded"
					);
			}
			conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, false);
			CodecTestWithNOCompression(conf, "org.apache.hadoop.io.compress.DefaultCodec");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCodecPoolCompressorReinit()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, true);
			if (ZlibFactory.IsNativeZlibLoaded(conf))
			{
				GzipCodec gzc = ReflectionUtils.NewInstance<GzipCodec>(conf);
				GzipReinitTest(conf, gzc);
			}
			else
			{
				Log.Warn("testCodecPoolCompressorReinit skipped: native libs not loaded");
			}
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, false);
			DefaultCodec dfc = ReflectionUtils.NewInstance<DefaultCodec>(conf);
			GzipReinitTest(conf, dfc);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		[Fact]
		public virtual void TestSequenceFileDefaultCodec()
		{
			SequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DefaultCodec", 100
				);
			SequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DefaultCodec", 
				1000000);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		public virtual void TestSequenceFileBZip2Codec()
		{
			Configuration conf = new Configuration();
			conf.Set("io.compression.codec.bzip2.library", "java-builtin");
			SequenceFileCodecTest(conf, 0, "org.apache.hadoop.io.compress.BZip2Codec", 100);
			SequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.BZip2Codec", 100);
			SequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.BZip2Codec", 1000000
				);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		public virtual void TestSequenceFileBZip2NativeCodec()
		{
			Configuration conf = new Configuration();
			conf.Set("io.compression.codec.bzip2.library", "system-native");
			if (NativeCodeLoader.IsNativeCodeLoaded())
			{
				if (Bzip2Factory.IsNativeBzip2Loaded(conf))
				{
					SequenceFileCodecTest(conf, 0, "org.apache.hadoop.io.compress.BZip2Codec", 100);
					SequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.BZip2Codec", 100);
					SequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.BZip2Codec", 1000000
						);
				}
				else
				{
					Log.Warn("Native hadoop library available but native bzip2 is not");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		[Fact]
		public virtual void TestSequenceFileDeflateCodec()
		{
			SequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DeflateCodec", 100
				);
			SequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DeflateCodec", 
				1000000);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		private static void SequenceFileCodecTest(Configuration conf, int lines, string codecClass
			, int blockSize)
		{
			Path filePath = new Path("SequenceFileCodecTest." + codecClass);
			// Configuration
			conf.SetInt("io.seqfile.compress.blocksize", blockSize);
			// Create the SequenceFile
			FileSystem fs = FileSystem.Get(conf);
			Log.Info("Creating SequenceFile with codec \"" + codecClass + "\"");
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, filePath, typeof(
				Text), typeof(Text), SequenceFile.CompressionType.Block, (CompressionCodec)System.Activator.CreateInstance
				(Runtime.GetType(codecClass)));
			// Write some data
			Log.Info("Writing to SequenceFile...");
			for (int i = 0; i < lines; i++)
			{
				Text key = new Text("key" + i);
				Text value = new Text("value" + i);
				writer.Append(key, value);
			}
			writer.Close();
			// Read the data back and check
			Log.Info("Reading from the SequenceFile...");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, conf);
			Writable key_1 = (Writable)System.Activator.CreateInstance(reader.GetKeyClass());
			Writable value_1 = (Writable)System.Activator.CreateInstance(reader.GetValueClass
				());
			int lc = 0;
			try
			{
				while (reader.Next(key_1, value_1))
				{
					Assert.Equal("key" + lc, key_1.ToString());
					Assert.Equal("value" + lc, value_1.ToString());
					lc++;
				}
			}
			finally
			{
				reader.Close();
			}
			Assert.Equal(lines, lc);
			// Delete temporary files
			fs.Delete(filePath, false);
			Log.Info("SUCCESS! Completed SequenceFileCodecTest with codec \"" + codecClass + 
				"\"");
		}

		/// <summary>
		/// Regression test for HADOOP-8423: seeking in a block-compressed
		/// stream would not properly reset the block decompressor state.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSnappyMapFile()
		{
			Assume.AssumeTrue(SnappyCodec.IsNativeCodeLoaded());
			CodecTestMapFile(typeof(SnappyCodec), SequenceFile.CompressionType.Block, 100);
		}

		/// <exception cref="System.Exception"/>
		private void CodecTestMapFile(Type clazz, SequenceFile.CompressionType type, int 
			records)
		{
			FileSystem fs = FileSystem.Get(conf);
			Log.Info("Creating MapFiles with " + records + " records using codec " + clazz.Name
				);
			Path path = new Path(new Path(Runtime.GetProperty("test.build.data", "/tmp")), clazz
				.Name + "-" + type + "-" + records);
			Log.Info("Writing " + path);
			CreateMapFile(conf, fs, path, System.Activator.CreateInstance(clazz), type, records
				);
			MapFile.Reader reader = new MapFile.Reader(path, conf);
			Text key1 = new Text("002");
			NUnit.Framework.Assert.IsNotNull(reader.Get(key1, new Text()));
			Text key2 = new Text("004");
			NUnit.Framework.Assert.IsNotNull(reader.Get(key2, new Text()));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateMapFile(Configuration conf, FileSystem fs, Path path, CompressionCodec
			 codec, SequenceFile.CompressionType type, int records)
		{
			MapFile.Writer writer = new MapFile.Writer(conf, path, MapFile.Writer.KeyClass(typeof(
				Text)), MapFile.Writer.ValueClass(typeof(Text)), MapFile.Writer.Compression(type
				, codec));
			Text key = new Text();
			for (int j = 0; j < records; j++)
			{
				key.Set(string.Format("%03d", j));
				writer.Append(key, key);
			}
			writer.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			int count = 10000;
			string codecClass = "org.apache.hadoop.io.compress.DefaultCodec";
			string usage = "TestCodec [-count N] [-codec <codec class>]";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			for (int i = 0; i < args.Length; ++i)
			{
				// parse command line
				if (args[i] == null)
				{
					continue;
				}
				else
				{
					if (args[i].Equals("-count"))
					{
						count = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-codec"))
						{
							codecClass = args[++i];
						}
					}
				}
			}
			Configuration conf = new Configuration();
			int seed = 0;
			// Note that exceptions will propagate out.
			CodecTest(conf, seed, count, codecClass);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGzipCompatibility()
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			Log.Info("seed: " + seed);
			DataOutputBuffer dflbuf = new DataOutputBuffer();
			GZIPOutputStream gzout = new GZIPOutputStream(dflbuf);
			byte[] b = new byte[r.Next(128 * 1024 + 1)];
			r.NextBytes(b);
			gzout.Write(b);
			gzout.Close();
			DataInputBuffer gzbuf = new DataInputBuffer();
			gzbuf.Reset(dflbuf.GetData(), dflbuf.GetLength());
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, false);
			CompressionCodec codec = ReflectionUtils.NewInstance<GzipCodec>(conf);
			Decompressor decom = codec.CreateDecompressor();
			NUnit.Framework.Assert.IsNotNull(decom);
			Assert.Equal(typeof(BuiltInGzipDecompressor), decom.GetType());
			InputStream gzin = codec.CreateInputStream(gzbuf, decom);
			dflbuf.Reset();
			IOUtils.CopyBytes(gzin, dflbuf, 4096);
			byte[] dflchk = Arrays.CopyOf(dflbuf.GetData(), dflbuf.GetLength());
			Assert.AssertArrayEquals(b, dflchk);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void GzipConcatTest(Configuration conf, Type decomClass)
		{
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			Log.Info(decomClass + " seed: " + seed);
			int Concat = r.Next(4) + 3;
			int Buflen = 128 * 1024;
			DataOutputBuffer dflbuf = new DataOutputBuffer();
			DataOutputBuffer chkbuf = new DataOutputBuffer();
			byte[] b = new byte[Buflen];
			for (int i = 0; i < Concat; ++i)
			{
				GZIPOutputStream gzout = new GZIPOutputStream(dflbuf);
				r.NextBytes(b);
				int len = r.Next(Buflen);
				int off = r.Next(Buflen - len);
				chkbuf.Write(b, off, len);
				gzout.Write(b, off, len);
				gzout.Close();
			}
			byte[] chk = Arrays.CopyOf(chkbuf.GetData(), chkbuf.GetLength());
			CompressionCodec codec = ReflectionUtils.NewInstance<GzipCodec>(conf);
			Decompressor decom = codec.CreateDecompressor();
			NUnit.Framework.Assert.IsNotNull(decom);
			Assert.Equal(decomClass, decom.GetType());
			DataInputBuffer gzbuf = new DataInputBuffer();
			gzbuf.Reset(dflbuf.GetData(), dflbuf.GetLength());
			InputStream gzin = codec.CreateInputStream(gzbuf, decom);
			dflbuf.Reset();
			IOUtils.CopyBytes(gzin, dflbuf, 4096);
			byte[] dflchk = Arrays.CopyOf(dflbuf.GetData(), dflbuf.GetLength());
			Assert.AssertArrayEquals(chk, dflchk);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestBuiltInGzipConcat()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, false);
			GzipConcatTest(conf, typeof(BuiltInGzipDecompressor));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestNativeGzipConcat()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, true);
			if (!ZlibFactory.IsNativeZlibLoaded(conf))
			{
				Log.Warn("skipped: native libs not loaded");
				return;
			}
			GzipConcatTest(conf, typeof(GzipCodec.GzipZlibDecompressor));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGzipCodecRead()
		{
			// Create a gzipped file and try to read it back, using a decompressor
			// from the CodecPool.
			// Don't use native libs for this test.
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, false);
			NUnit.Framework.Assert.IsFalse("ZlibFactory is using native libs against request"
				, ZlibFactory.IsNativeZlibLoaded(conf));
			// Ensure that the CodecPool has a BuiltInZlibInflater in it.
			Decompressor zlibDecompressor = ZlibFactory.GetZlibDecompressor(conf);
			NUnit.Framework.Assert.IsNotNull("zlibDecompressor is null!", zlibDecompressor);
			Assert.True("ZlibFactory returned unexpected inflator", zlibDecompressor
				 is BuiltInZlibInflater);
			CodecPool.ReturnDecompressor(zlibDecompressor);
			// Now create a GZip text file.
			string tmpDir = Runtime.GetProperty("test.build.data", "/tmp/");
			Path f = new Path(new Path(tmpDir), "testGzipCodecRead.txt.gz");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream
				(new FileOutputStream(f.ToString()))));
			string msg = "This is the message in the file!";
			bw.Write(msg);
			bw.Close();
			// Now read it back, using the CodecPool to establish the
			// decompressor to use.
			CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
			CompressionCodec codec = ccf.GetCodec(f);
			Decompressor decompressor = CodecPool.GetDecompressor(codec);
			FileSystem fs = FileSystem.GetLocal(conf);
			InputStream @is = fs.Open(f);
			@is = codec.CreateInputStream(@is, decompressor);
			BufferedReader br = new BufferedReader(new InputStreamReader(@is));
			string line = br.ReadLine();
			Assert.Equal("Didn't get the same message back!", msg, line);
			br.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyGzipFile(string filename, string msg)
		{
			BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new 
				FileInputStream(filename))));
			try
			{
				string line = r.ReadLine();
				Assert.Equal("Got invalid line back from " + filename, msg, line
					);
			}
			finally
			{
				r.Close();
				new FilePath(filename).Delete();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGzipLongOverflow()
		{
			Log.Info("testGzipLongOverflow");
			// Don't use native libs for this test.
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, false);
			NUnit.Framework.Assert.IsFalse("ZlibFactory is using native libs against request"
				, ZlibFactory.IsNativeZlibLoaded(conf));
			// Ensure that the CodecPool has a BuiltInZlibInflater in it.
			Decompressor zlibDecompressor = ZlibFactory.GetZlibDecompressor(conf);
			NUnit.Framework.Assert.IsNotNull("zlibDecompressor is null!", zlibDecompressor);
			Assert.True("ZlibFactory returned unexpected inflator", zlibDecompressor
				 is BuiltInZlibInflater);
			CodecPool.ReturnDecompressor(zlibDecompressor);
			// Now create a GZip text file.
			string tmpDir = Runtime.GetProperty("test.build.data", "/tmp/");
			Path f = new Path(new Path(tmpDir), "testGzipLongOverflow.bin.gz");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream
				(new FileOutputStream(f.ToString()))));
			int Nbuf = 1024 * 4 + 1;
			char[] buf = new char[1024 * 1024];
			for (int i = 0; i < buf.Length; i++)
			{
				buf[i] = '\0';
			}
			for (int i_1 = 0; i_1 < Nbuf; i_1++)
			{
				bw.Write(buf);
			}
			bw.Close();
			// Now read it back, using the CodecPool to establish the
			// decompressor to use.
			CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
			CompressionCodec codec = ccf.GetCodec(f);
			Decompressor decompressor = CodecPool.GetDecompressor(codec);
			FileSystem fs = FileSystem.GetLocal(conf);
			InputStream @is = fs.Open(f);
			@is = codec.CreateInputStream(@is, decompressor);
			BufferedReader br = new BufferedReader(new InputStreamReader(@is));
			for (int j = 0; j < Nbuf; j++)
			{
				int n = br.Read(buf);
				Assert.Equal("got wrong read length!", n, buf.Length);
				for (int i_2 = 0; i_2 < buf.Length; i_2++)
				{
					Assert.Equal("got wrong byte!", buf[i_2], '\0');
				}
			}
			br.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGzipCodecWrite(bool useNative)
		{
			// Create a gzipped file using a compressor from the CodecPool,
			// and try to read it back via the regular GZIPInputStream.
			// Use native libs per the parameter
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.IoNativeLibAvailableKey, useNative);
			if (useNative)
			{
				if (!ZlibFactory.IsNativeZlibLoaded(conf))
				{
					Log.Warn("testGzipCodecWrite skipped: native libs not loaded");
					return;
				}
			}
			else
			{
				NUnit.Framework.Assert.IsFalse("ZlibFactory is using native libs against request"
					, ZlibFactory.IsNativeZlibLoaded(conf));
			}
			// Ensure that the CodecPool has a BuiltInZlibDeflater in it.
			Compressor zlibCompressor = ZlibFactory.GetZlibCompressor(conf);
			NUnit.Framework.Assert.IsNotNull("zlibCompressor is null!", zlibCompressor);
			Assert.True("ZlibFactory returned unexpected deflator", useNative
				 ? zlibCompressor is ZlibCompressor : zlibCompressor is BuiltInZlibDeflater);
			CodecPool.ReturnCompressor(zlibCompressor);
			// Create a GZIP text file via the Compressor interface.
			CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
			CompressionCodec codec = ccf.GetCodec(new Path("foo.gz"));
			Assert.True("Codec for .gz file is not GzipCodec", codec is GzipCodec
				);
			string msg = "This is the message we are going to compress.";
			string tmpDir = Runtime.GetProperty("test.build.data", "/tmp/");
			string fileName = new Path(new Path(tmpDir), "testGzipCodecWrite.txt.gz").ToString
				();
			BufferedWriter w = null;
			Compressor gzipCompressor = CodecPool.GetCompressor(codec);
			if (null != gzipCompressor)
			{
				// If it gives us back a Compressor, we should be able to use this
				// to write files we can then read back with Java's gzip tools.
				OutputStream os = new CompressorStream(new FileOutputStream(fileName), gzipCompressor
					);
				w = new BufferedWriter(new OutputStreamWriter(os));
				w.Write(msg);
				w.Close();
				CodecPool.ReturnCompressor(gzipCompressor);
				VerifyGzipFile(fileName, msg);
			}
			// Create a gzip text file via codec.getOutputStream().
			w = new BufferedWriter(new OutputStreamWriter(codec.CreateOutputStream(new FileOutputStream
				(fileName))));
			w.Write(msg);
			w.Close();
			VerifyGzipFile(fileName, msg);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGzipCodecWriteJava()
		{
			TestGzipCodecWrite(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGzipNativeCodecWrite()
		{
			TestGzipCodecWrite(true);
		}

		public virtual void TestCodecPoolAndGzipDecompressor()
		{
			// BuiltInZlibInflater should not be used as the GzipCodec decompressor.
			// Assert that this is the case.
			// Don't use native libs for this test.
			Configuration conf = new Configuration();
			conf.SetBoolean("hadoop.native.lib", false);
			NUnit.Framework.Assert.IsFalse("ZlibFactory is using native libs against request"
				, ZlibFactory.IsNativeZlibLoaded(conf));
			// This should give us a BuiltInZlibInflater.
			Decompressor zlibDecompressor = ZlibFactory.GetZlibDecompressor(conf);
			NUnit.Framework.Assert.IsNotNull("zlibDecompressor is null!", zlibDecompressor);
			Assert.True("ZlibFactory returned unexpected inflator", zlibDecompressor
				 is BuiltInZlibInflater);
			// its createOutputStream() just wraps the existing stream in a
			// java.util.zip.GZIPOutputStream.
			CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
			CompressionCodec codec = ccf.GetCodec(new Path("foo.gz"));
			Assert.True("Codec for .gz file is not GzipCodec", codec is GzipCodec
				);
			// make sure we don't get a null decompressor
			Decompressor codecDecompressor = codec.CreateDecompressor();
			if (null == codecDecompressor)
			{
				NUnit.Framework.Assert.Fail("Got null codecDecompressor");
			}
			// Asking the CodecPool for a decompressor for GzipCodec
			// should not return null
			Decompressor poolDecompressor = CodecPool.GetDecompressor(codec);
			if (null == poolDecompressor)
			{
				NUnit.Framework.Assert.Fail("Got null poolDecompressor");
			}
			// return a couple decompressors
			CodecPool.ReturnDecompressor(zlibDecompressor);
			CodecPool.ReturnDecompressor(poolDecompressor);
			Decompressor poolDecompressor2 = CodecPool.GetDecompressor(codec);
			if (poolDecompressor.GetType() == typeof(BuiltInGzipDecompressor))
			{
				if (poolDecompressor == poolDecompressor2)
				{
					NUnit.Framework.Assert.Fail("Reused java gzip decompressor in pool");
				}
			}
			else
			{
				if (poolDecompressor != poolDecompressor2)
				{
					NUnit.Framework.Assert.Fail("Did not reuse native gzip decompressor in pool");
				}
			}
		}
	}
}
