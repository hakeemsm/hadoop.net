using Sharpen;

namespace org.apache.hadoop.io.compress
{
	public class TestCodec
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodec
			)));

		private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		private int count = 10000;

		private int seed = new java.util.Random().nextInt();

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDefaultCodec()
		{
			codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DefaultCodec");
			codecTest(conf, seed, count, "org.apache.hadoop.io.compress.DefaultCodec");
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGzipCodec()
		{
			codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
			codecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testBZip2Codec()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("io.compression.codec.bzip2.library", "java-builtin");
			codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
			codecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testBZip2NativeCodec()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("io.compression.codec.bzip2.library", "system-native");
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				if (org.apache.hadoop.io.compress.bzip2.Bzip2Factory.isNativeBzip2Loaded(conf))
				{
					codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
					codecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
					conf.set("io.compression.codec.bzip2.library", "java-builtin");
					codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.BZip2Codec");
					codecTest(conf, seed, count, "org.apache.hadoop.io.compress.BZip2Codec");
				}
				else
				{
					LOG.warn("Native hadoop library available but native bzip2 is not");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testSnappyCodec()
		{
			if (org.apache.hadoop.io.compress.SnappyCodec.isNativeCodeLoaded())
			{
				codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.SnappyCodec");
				codecTest(conf, seed, count, "org.apache.hadoop.io.compress.SnappyCodec");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testLz4Codec()
		{
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				if (org.apache.hadoop.io.compress.Lz4Codec.isNativeCodeLoaded())
				{
					conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY
						, false);
					codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
					codecTest(conf, seed, count, "org.apache.hadoop.io.compress.Lz4Codec");
					conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY
						, true);
					codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.Lz4Codec");
					codecTest(conf, seed, count, "org.apache.hadoop.io.compress.Lz4Codec");
				}
				else
				{
					NUnit.Framework.Assert.Fail("Native hadoop library available but lz4 not");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDeflateCodec()
		{
			codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.DeflateCodec");
			codecTest(conf, seed, count, "org.apache.hadoop.io.compress.DeflateCodec");
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGzipCodecWithParam()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(this.conf);
			org.apache.hadoop.io.compress.zlib.ZlibFactory.setCompressionLevel(conf, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				.BEST_COMPRESSION);
			org.apache.hadoop.io.compress.zlib.ZlibFactory.setCompressionStrategy(conf, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				.HUFFMAN_ONLY);
			codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.GzipCodec");
			codecTest(conf, seed, count, "org.apache.hadoop.io.compress.GzipCodec");
		}

		/// <exception cref="System.IO.IOException"/>
		private static void codecTest(org.apache.hadoop.conf.Configuration conf, int seed
			, int count, string codecClass)
		{
			// Create the codec
			org.apache.hadoop.io.compress.CompressionCodec codec = null;
			try
			{
				codec = (org.apache.hadoop.io.compress.CompressionCodec)org.apache.hadoop.util.ReflectionUtils
					.newInstance(conf.getClassByName(codecClass), conf);
			}
			catch (java.lang.ClassNotFoundException)
			{
				throw new System.IO.IOException("Illegal codec!");
			}
			LOG.info("Created a Codec object of type: " + codecClass);
			// Generate data
			org.apache.hadoop.io.DataOutputBuffer data = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				(seed);
			for (int i = 0; i < count; ++i)
			{
				generator.next();
				org.apache.hadoop.io.RandomDatum key = generator.getKey();
				org.apache.hadoop.io.RandomDatum value = generator.getValue();
				key.write(data);
				value.write(data);
			}
			LOG.info("Generated " + count + " records");
			// Compress data
			org.apache.hadoop.io.DataOutputBuffer compressedDataBuffer = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.compress.CompressionOutputStream deflateFilter = codec.createOutputStream
				(compressedDataBuffer);
			java.io.DataOutputStream deflateOut = new java.io.DataOutputStream(new java.io.BufferedOutputStream
				(deflateFilter));
			deflateOut.write(data.getData(), 0, data.getLength());
			deflateOut.flush();
			deflateFilter.finish();
			LOG.info("Finished compressing data");
			// De-compress data
			org.apache.hadoop.io.DataInputBuffer deCompressedDataBuffer = new org.apache.hadoop.io.DataInputBuffer
				();
			deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, compressedDataBuffer
				.getLength());
			org.apache.hadoop.io.compress.CompressionInputStream inflateFilter = codec.createInputStream
				(deCompressedDataBuffer);
			java.io.DataInputStream inflateIn = new java.io.DataInputStream(new java.io.BufferedInputStream
				(inflateFilter));
			// Check
			org.apache.hadoop.io.DataInputBuffer originalData = new org.apache.hadoop.io.DataInputBuffer
				();
			originalData.reset(data.getData(), 0, data.getLength());
			java.io.DataInputStream originalIn = new java.io.DataInputStream(new java.io.BufferedInputStream
				(originalData));
			for (int i_1 = 0; i_1 < count; ++i_1)
			{
				org.apache.hadoop.io.RandomDatum k1 = new org.apache.hadoop.io.RandomDatum();
				org.apache.hadoop.io.RandomDatum v1 = new org.apache.hadoop.io.RandomDatum();
				k1.readFields(originalIn);
				v1.readFields(originalIn);
				org.apache.hadoop.io.RandomDatum k2 = new org.apache.hadoop.io.RandomDatum();
				org.apache.hadoop.io.RandomDatum v2 = new org.apache.hadoop.io.RandomDatum();
				k2.readFields(inflateIn);
				v2.readFields(inflateIn);
				NUnit.Framework.Assert.IsTrue("original and compressed-then-decompressed-output not equal"
					, k1.Equals(k2) && v1.Equals(v2));
				// original and compressed-then-decompressed-output have the same hashCode
				System.Collections.Generic.IDictionary<org.apache.hadoop.io.RandomDatum, string> 
					m = new System.Collections.Generic.Dictionary<org.apache.hadoop.io.RandomDatum, 
					string>();
				m[k1] = k1.ToString();
				m[v1] = v1.ToString();
				string result = m[k2];
				NUnit.Framework.Assert.AreEqual("k1 and k2 hashcode not equal", result, k1.ToString
					());
				result = m[v2];
				NUnit.Framework.Assert.AreEqual("v1 and v2 hashcode not equal", result, v1.ToString
					());
			}
			// De-compress data byte-at-a-time
			originalData.reset(data.getData(), 0, data.getLength());
			deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, compressedDataBuffer
				.getLength());
			inflateFilter = codec.createInputStream(deCompressedDataBuffer);
			// Check
			originalIn = new java.io.DataInputStream(new java.io.BufferedInputStream(originalData
				));
			int expected;
			do
			{
				expected = originalIn.read();
				NUnit.Framework.Assert.AreEqual("Inflated stream read by byte does not match", expected
					, inflateFilter.read());
			}
			while (expected != -1);
			LOG.info("SUCCESS! Completed checking " + count + " records");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSplitableCodecs()
		{
			testSplitableCodec(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.BZip2Codec
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		private void testSplitableCodec(java.lang.Class codecClass)
		{
			long DEFLBYTES = 2 * 1024 * 1024;
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			java.util.Random rand = new java.util.Random();
			long seed = rand.nextLong();
			LOG.info("seed: " + seed);
			rand.setSeed(seed);
			org.apache.hadoop.io.compress.SplittableCompressionCodec codec = org.apache.hadoop.util.ReflectionUtils
				.newInstance(codecClass, conf);
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.FileStatus infile = fs.getFileStatus(writeSplitTestFile(fs, 
				rand, codec, DEFLBYTES));
			if (infile.getLen() > int.MaxValue)
			{
				NUnit.Framework.Assert.Fail("Unexpected compression: " + DEFLBYTES + " -> " + infile
					.getLen());
			}
			int flen = (int)infile.getLen();
			org.apache.hadoop.io.Text line = new org.apache.hadoop.io.Text();
			org.apache.hadoop.io.compress.Decompressor dcmp = org.apache.hadoop.io.compress.CodecPool
				.getDecompressor(codec);
			try
			{
				for (int pos = 0; pos < infile.getLen(); pos += rand.nextInt(flen / 8))
				{
					// read from random positions, verifying that there exist two sequential
					// lines as written in writeSplitTestFile
					org.apache.hadoop.io.compress.SplitCompressionInputStream @in = codec.createInputStream
						(fs.open(infile.getPath()), dcmp, pos, flen, org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE
						.BYBLOCK);
					if (@in.getAdjustedStart() >= flen)
					{
						break;
					}
					LOG.info("SAMPLE " + @in.getAdjustedStart() + "," + @in.getAdjustedEnd());
					org.apache.hadoop.util.LineReader lreader = new org.apache.hadoop.util.LineReader
						(@in);
					lreader.readLine(line);
					// ignore; likely partial
					if (@in.getPos() >= flen)
					{
						break;
					}
					lreader.readLine(line);
					int seq1 = readLeadingInt(line);
					lreader.readLine(line);
					if (@in.getPos() >= flen)
					{
						break;
					}
					int seq2 = readLeadingInt(line);
					NUnit.Framework.Assert.AreEqual("Mismatched lines", seq1 + 1, seq2);
				}
			}
			finally
			{
				org.apache.hadoop.io.compress.CodecPool.returnDecompressor(dcmp);
			}
			// remove on success
			fs.delete(infile.getPath().getParent(), true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static int readLeadingInt(org.apache.hadoop.io.Text txt)
		{
			java.io.DataInputStream @in = new java.io.DataInputStream(new java.io.ByteArrayInputStream
				(txt.getBytes()));
			return @in.readInt();
		}

		/// <summary>Write infLen bytes (deflated) to file in test dir using codec.</summary>
		/// <remarks>
		/// Write infLen bytes (deflated) to file in test dir using codec.
		/// Records are of the form
		/// &lt;i&gt;&lt;b64 rand&gt;&lt;i+i&gt;&lt;b64 rand&gt;
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.fs.Path writeSplitTestFile(org.apache.hadoop.fs.FileSystem
			 fs, java.util.Random rand, org.apache.hadoop.io.compress.CompressionCodec codec
			, long infLen)
		{
			int REC_SIZE = 1024;
			org.apache.hadoop.fs.Path wd = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path
				(Sharpen.Runtime.getProperty("test.build.data", "/tmp")).makeQualified(fs), Sharpen.Runtime.getClassForObject
				(codec).getSimpleName());
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(wd, "test" + codec
				.getDefaultExtension());
			byte[] b = new byte[REC_SIZE];
			org.apache.commons.codec.binary.Base64 b64 = new org.apache.commons.codec.binary.Base64
				(0, null);
			java.io.DataOutputStream fout = null;
			org.apache.hadoop.io.compress.Compressor cmp = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(codec);
			try
			{
				fout = new java.io.DataOutputStream(codec.createOutputStream(fs.create(file, true
					), cmp));
				org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
					(REC_SIZE * 4 / 3 + 4);
				int seq = 0;
				while (infLen > 0)
				{
					rand.nextBytes(b);
					byte[] b64enc = b64.encode(b);
					// ensures rand printable, no LF
					dob.reset();
					dob.writeInt(seq);
					System.Array.Copy(dob.getData(), 0, b64enc, 0, dob.getLength());
					fout.write(b64enc);
					fout.write('\n');
					++seq;
					infLen -= b64enc.Length;
				}
				LOG.info("Wrote " + seq + " records to " + file);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, fout);
				org.apache.hadoop.io.compress.CodecPool.returnCompressor(cmp);
			}
			return file;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCodecPoolGzipReuse()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, true);
			if (!org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf))
			{
				LOG.warn("testCodecPoolGzipReuse skipped: native libs not loaded");
				return;
			}
			org.apache.hadoop.io.compress.GzipCodec gzc = org.apache.hadoop.util.ReflectionUtils
				.newInstance<org.apache.hadoop.io.compress.GzipCodec>(conf);
			org.apache.hadoop.io.compress.DefaultCodec dfc = org.apache.hadoop.util.ReflectionUtils
				.newInstance<org.apache.hadoop.io.compress.DefaultCodec>(conf);
			org.apache.hadoop.io.compress.Compressor c1 = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(gzc);
			org.apache.hadoop.io.compress.Compressor c2 = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(dfc);
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(c1);
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(c2);
			NUnit.Framework.Assert.IsTrue("Got mismatched ZlibCompressor", c2 != org.apache.hadoop.io.compress.CodecPool
				.getCompressor(gzc));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void gzipReinitTest(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.io.compress.CompressionCodec
			 codec)
		{
			// Add codec to cache
			org.apache.hadoop.io.compress.zlib.ZlibFactory.setCompressionLevel(conf, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				.BEST_COMPRESSION);
			org.apache.hadoop.io.compress.zlib.ZlibFactory.setCompressionStrategy(conf, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy
				.DEFAULT_STRATEGY);
			org.apache.hadoop.io.compress.Compressor c1 = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(codec);
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(c1);
			// reset compressor's compression level to perform no compression
			org.apache.hadoop.io.compress.zlib.ZlibFactory.setCompressionLevel(conf, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				.NO_COMPRESSION);
			org.apache.hadoop.io.compress.Compressor c2 = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(codec, conf);
			// ensure same compressor placed earlier
			NUnit.Framework.Assert.IsTrue("Got mismatched ZlibCompressor", c1 == c2);
			java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
			org.apache.hadoop.io.compress.CompressionOutputStream cos = null;
			// write trivially compressable data
			byte[] b = new byte[1 << 15];
			java.util.Arrays.fill(b, unchecked((byte)43));
			try
			{
				cos = codec.createOutputStream(bos, c2);
				cos.write(b);
			}
			finally
			{
				if (cos != null)
				{
					cos.close();
				}
				org.apache.hadoop.io.compress.CodecPool.returnCompressor(c2);
			}
			byte[] outbytes = bos.toByteArray();
			// verify data were not compressed
			NUnit.Framework.Assert.IsTrue("Compressed bytes contrary to configuration", outbytes
				.Length >= b.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void codecTestWithNOCompression(org.apache.hadoop.conf.Configuration
			 conf, string codecClass)
		{
			// Create a compressor with NO_COMPRESSION and make sure that
			// output is not compressed by comparing the size with the
			// original input
			org.apache.hadoop.io.compress.CompressionCodec codec = null;
			org.apache.hadoop.io.compress.zlib.ZlibFactory.setCompressionLevel(conf, org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
				.NO_COMPRESSION);
			try
			{
				codec = (org.apache.hadoop.io.compress.CompressionCodec)org.apache.hadoop.util.ReflectionUtils
					.newInstance(conf.getClassByName(codecClass), conf);
			}
			catch (java.lang.ClassNotFoundException)
			{
				throw new System.IO.IOException("Illegal codec!");
			}
			org.apache.hadoop.io.compress.Compressor c = codec.createCompressor();
			// ensure same compressor placed earlier
			java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
			org.apache.hadoop.io.compress.CompressionOutputStream cos = null;
			// write trivially compressable data
			byte[] b = new byte[1 << 15];
			java.util.Arrays.fill(b, unchecked((byte)43));
			try
			{
				cos = codec.createOutputStream(bos, c);
				cos.write(b);
			}
			finally
			{
				if (cos != null)
				{
					cos.close();
				}
			}
			byte[] outbytes = bos.toByteArray();
			// verify data were not compressed
			NUnit.Framework.Assert.IsTrue("Compressed bytes contrary to configuration(NO_COMPRESSION)"
				, outbytes.Length >= b.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCodecInitWithCompressionLevel()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, true);
			if (org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf))
			{
				LOG.info("testCodecInitWithCompressionLevel with native");
				codecTestWithNOCompression(conf, "org.apache.hadoop.io.compress.GzipCodec");
				codecTestWithNOCompression(conf, "org.apache.hadoop.io.compress.DefaultCodec");
			}
			else
			{
				LOG.warn("testCodecInitWithCompressionLevel for native skipped" + ": native libs not loaded"
					);
			}
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, false);
			codecTestWithNOCompression(conf, "org.apache.hadoop.io.compress.DefaultCodec");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCodecPoolCompressorReinit()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, true);
			if (org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf))
			{
				org.apache.hadoop.io.compress.GzipCodec gzc = org.apache.hadoop.util.ReflectionUtils
					.newInstance<org.apache.hadoop.io.compress.GzipCodec>(conf);
				gzipReinitTest(conf, gzc);
			}
			else
			{
				LOG.warn("testCodecPoolCompressorReinit skipped: native libs not loaded");
			}
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, false);
			org.apache.hadoop.io.compress.DefaultCodec dfc = org.apache.hadoop.util.ReflectionUtils
				.newInstance<org.apache.hadoop.io.compress.DefaultCodec>(conf);
			gzipReinitTest(conf, dfc);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		[NUnit.Framework.Test]
		public virtual void testSequenceFileDefaultCodec()
		{
			sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DefaultCodec", 100
				);
			sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DefaultCodec", 
				1000000);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		public virtual void testSequenceFileBZip2Codec()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("io.compression.codec.bzip2.library", "java-builtin");
			sequenceFileCodecTest(conf, 0, "org.apache.hadoop.io.compress.BZip2Codec", 100);
			sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.BZip2Codec", 100);
			sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.BZip2Codec", 1000000
				);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		public virtual void testSequenceFileBZip2NativeCodec()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("io.compression.codec.bzip2.library", "system-native");
			if (org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
			{
				if (org.apache.hadoop.io.compress.bzip2.Bzip2Factory.isNativeBzip2Loaded(conf))
				{
					sequenceFileCodecTest(conf, 0, "org.apache.hadoop.io.compress.BZip2Codec", 100);
					sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.BZip2Codec", 100);
					sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.BZip2Codec", 1000000
						);
				}
				else
				{
					LOG.warn("Native hadoop library available but native bzip2 is not");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		[NUnit.Framework.Test]
		public virtual void testSequenceFileDeflateCodec()
		{
			sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.DeflateCodec", 100
				);
			sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.DeflateCodec", 
				1000000);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.lang.ClassNotFoundException"/>
		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		private static void sequenceFileCodecTest(org.apache.hadoop.conf.Configuration conf
			, int lines, string codecClass, int blockSize)
		{
			org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path("SequenceFileCodecTest."
				 + codecClass);
			// Configuration
			conf.setInt("io.seqfile.compress.blocksize", blockSize);
			// Create the SequenceFile
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
			LOG.info("Creating SequenceFile with codec \"" + codecClass + "\"");
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(fs, conf, filePath, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType
				.BLOCK, (org.apache.hadoop.io.compress.CompressionCodec)java.lang.Class.forName(
				codecClass).newInstance());
			// Write some data
			LOG.info("Writing to SequenceFile...");
			for (int i = 0; i < lines; i++)
			{
				org.apache.hadoop.io.Text key = new org.apache.hadoop.io.Text("key" + i);
				org.apache.hadoop.io.Text value = new org.apache.hadoop.io.Text("value" + i);
				writer.append(key, value);
			}
			writer.close();
			// Read the data back and check
			LOG.info("Reading from the SequenceFile...");
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(fs, filePath, conf);
			org.apache.hadoop.io.Writable key_1 = (org.apache.hadoop.io.Writable)reader.getKeyClass
				().newInstance();
			org.apache.hadoop.io.Writable value_1 = (org.apache.hadoop.io.Writable)reader.getValueClass
				().newInstance();
			int lc = 0;
			try
			{
				while (reader.next(key_1, value_1))
				{
					NUnit.Framework.Assert.AreEqual("key" + lc, key_1.ToString());
					NUnit.Framework.Assert.AreEqual("value" + lc, value_1.ToString());
					lc++;
				}
			}
			finally
			{
				reader.close();
			}
			NUnit.Framework.Assert.AreEqual(lines, lc);
			// Delete temporary files
			fs.delete(filePath, false);
			LOG.info("SUCCESS! Completed SequenceFileCodecTest with codec \"" + codecClass + 
				"\"");
		}

		/// <summary>
		/// Regression test for HADOOP-8423: seeking in a block-compressed
		/// stream would not properly reset the block decompressor state.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSnappyMapFile()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.io.compress.SnappyCodec.isNativeCodeLoaded
				());
			codecTestMapFile(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.SnappyCodec
				)), org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, 100);
		}

		/// <exception cref="System.Exception"/>
		private void codecTestMapFile(java.lang.Class clazz, org.apache.hadoop.io.SequenceFile.CompressionType
			 type, int records)
		{
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
			LOG.info("Creating MapFiles with " + records + " records using codec " + clazz.getSimpleName
				());
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path
				(Sharpen.Runtime.getProperty("test.build.data", "/tmp")), clazz.getSimpleName() 
				+ "-" + type + "-" + records);
			LOG.info("Writing " + path);
			createMapFile(conf, fs, path, clazz.newInstance(), type, records);
			org.apache.hadoop.io.MapFile.Reader reader = new org.apache.hadoop.io.MapFile.Reader
				(path, conf);
			org.apache.hadoop.io.Text key1 = new org.apache.hadoop.io.Text("002");
			NUnit.Framework.Assert.IsNotNull(reader.get(key1, new org.apache.hadoop.io.Text()
				));
			org.apache.hadoop.io.Text key2 = new org.apache.hadoop.io.Text("004");
			NUnit.Framework.Assert.IsNotNull(reader.get(key2, new org.apache.hadoop.io.Text()
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void createMapFile(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.fs.Path path, org.apache.hadoop.io.compress.CompressionCodec
			 codec, org.apache.hadoop.io.SequenceFile.CompressionType type, int records)
		{
			org.apache.hadoop.io.MapFile.Writer writer = new org.apache.hadoop.io.MapFile.Writer
				(conf, path, org.apache.hadoop.io.MapFile.Writer.keyClass(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.Text))), org.apache.hadoop.io.MapFile.Writer.valueClass
				(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text))), org.apache.hadoop.io.MapFile.Writer
				.compression(type, codec));
			org.apache.hadoop.io.Text key = new org.apache.hadoop.io.Text();
			for (int j = 0; j < records; j++)
			{
				key.set(string.format("%03d", j));
				writer.append(key, key);
			}
			writer.close();
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
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			int seed = 0;
			// Note that exceptions will propagate out.
			codecTest(conf, seed, count, codecClass);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGzipCompatibility()
		{
			java.util.Random r = new java.util.Random();
			long seed = r.nextLong();
			r.setSeed(seed);
			LOG.info("seed: " + seed);
			org.apache.hadoop.io.DataOutputBuffer dflbuf = new org.apache.hadoop.io.DataOutputBuffer
				();
			java.util.zip.GZIPOutputStream gzout = new java.util.zip.GZIPOutputStream(dflbuf);
			byte[] b = new byte[r.nextInt(128 * 1024 + 1)];
			r.nextBytes(b);
			gzout.write(b);
			gzout.close();
			org.apache.hadoop.io.DataInputBuffer gzbuf = new org.apache.hadoop.io.DataInputBuffer
				();
			gzbuf.reset(dflbuf.getData(), dflbuf.getLength());
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, false);
			org.apache.hadoop.io.compress.CompressionCodec codec = org.apache.hadoop.util.ReflectionUtils
				.newInstance<org.apache.hadoop.io.compress.GzipCodec>(conf);
			org.apache.hadoop.io.compress.Decompressor decom = codec.createDecompressor();
			NUnit.Framework.Assert.IsNotNull(decom);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
				)), Sharpen.Runtime.getClassForObject(decom));
			java.io.InputStream gzin = codec.createInputStream(gzbuf, decom);
			dflbuf.reset();
			org.apache.hadoop.io.IOUtils.copyBytes(gzin, dflbuf, 4096);
			byte[] dflchk = java.util.Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
			NUnit.Framework.Assert.assertArrayEquals(b, dflchk);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void GzipConcatTest(org.apache.hadoop.conf.Configuration conf, java.lang.Class
			 decomClass)
		{
			java.util.Random r = new java.util.Random();
			long seed = r.nextLong();
			r.setSeed(seed);
			LOG.info(decomClass + " seed: " + seed);
			int CONCAT = r.nextInt(4) + 3;
			int BUFLEN = 128 * 1024;
			org.apache.hadoop.io.DataOutputBuffer dflbuf = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataOutputBuffer chkbuf = new org.apache.hadoop.io.DataOutputBuffer
				();
			byte[] b = new byte[BUFLEN];
			for (int i = 0; i < CONCAT; ++i)
			{
				java.util.zip.GZIPOutputStream gzout = new java.util.zip.GZIPOutputStream(dflbuf);
				r.nextBytes(b);
				int len = r.nextInt(BUFLEN);
				int off = r.nextInt(BUFLEN - len);
				chkbuf.write(b, off, len);
				gzout.write(b, off, len);
				gzout.close();
			}
			byte[] chk = java.util.Arrays.copyOf(chkbuf.getData(), chkbuf.getLength());
			org.apache.hadoop.io.compress.CompressionCodec codec = org.apache.hadoop.util.ReflectionUtils
				.newInstance<org.apache.hadoop.io.compress.GzipCodec>(conf);
			org.apache.hadoop.io.compress.Decompressor decom = codec.createDecompressor();
			NUnit.Framework.Assert.IsNotNull(decom);
			NUnit.Framework.Assert.AreEqual(decomClass, Sharpen.Runtime.getClassForObject(decom
				));
			org.apache.hadoop.io.DataInputBuffer gzbuf = new org.apache.hadoop.io.DataInputBuffer
				();
			gzbuf.reset(dflbuf.getData(), dflbuf.getLength());
			java.io.InputStream gzin = codec.createInputStream(gzbuf, decom);
			dflbuf.reset();
			org.apache.hadoop.io.IOUtils.copyBytes(gzin, dflbuf, 4096);
			byte[] dflchk = java.util.Arrays.copyOf(dflbuf.getData(), dflbuf.getLength());
			NUnit.Framework.Assert.assertArrayEquals(chk, dflchk);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testBuiltInGzipConcat()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, false);
			GzipConcatTest(conf, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testNativeGzipConcat()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, true);
			if (!org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf))
			{
				LOG.warn("skipped: native libs not loaded");
				return;
			}
			GzipConcatTest(conf, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec.GzipZlibDecompressor
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGzipCodecRead()
		{
			// Create a gzipped file and try to read it back, using a decompressor
			// from the CodecPool.
			// Don't use native libs for this test.
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, false);
			NUnit.Framework.Assert.IsFalse("ZlibFactory is using native libs against request"
				, org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf));
			// Ensure that the CodecPool has a BuiltInZlibInflater in it.
			org.apache.hadoop.io.compress.Decompressor zlibDecompressor = org.apache.hadoop.io.compress.zlib.ZlibFactory
				.getZlibDecompressor(conf);
			NUnit.Framework.Assert.IsNotNull("zlibDecompressor is null!", zlibDecompressor);
			NUnit.Framework.Assert.IsTrue("ZlibFactory returned unexpected inflator", zlibDecompressor
				 is org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater);
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(zlibDecompressor);
			// Now create a GZip text file.
			string tmpDir = Sharpen.Runtime.getProperty("test.build.data", "/tmp/");
			org.apache.hadoop.fs.Path f = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path
				(tmpDir), "testGzipCodecRead.txt.gz");
			java.io.BufferedWriter bw = new java.io.BufferedWriter(new java.io.OutputStreamWriter
				(new java.util.zip.GZIPOutputStream(new java.io.FileOutputStream(f.ToString())))
				);
			string msg = "This is the message in the file!";
			bw.write(msg);
			bw.close();
			// Now read it back, using the CodecPool to establish the
			// decompressor to use.
			org.apache.hadoop.io.compress.CompressionCodecFactory ccf = new org.apache.hadoop.io.compress.CompressionCodecFactory
				(conf);
			org.apache.hadoop.io.compress.CompressionCodec codec = ccf.getCodec(f);
			org.apache.hadoop.io.compress.Decompressor decompressor = org.apache.hadoop.io.compress.CodecPool
				.getDecompressor(codec);
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			java.io.InputStream @is = fs.open(f);
			@is = codec.createInputStream(@is, decompressor);
			java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader
				(@is));
			string line = br.readLine();
			NUnit.Framework.Assert.AreEqual("Didn't get the same message back!", msg, line);
			br.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void verifyGzipFile(string filename, string msg)
		{
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader
				(new java.util.zip.GZIPInputStream(new java.io.FileInputStream(filename))));
			try
			{
				string line = r.readLine();
				NUnit.Framework.Assert.AreEqual("Got invalid line back from " + filename, msg, line
					);
			}
			finally
			{
				r.close();
				new java.io.File(filename).delete();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGzipLongOverflow()
		{
			LOG.info("testGzipLongOverflow");
			// Don't use native libs for this test.
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, false);
			NUnit.Framework.Assert.IsFalse("ZlibFactory is using native libs against request"
				, org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf));
			// Ensure that the CodecPool has a BuiltInZlibInflater in it.
			org.apache.hadoop.io.compress.Decompressor zlibDecompressor = org.apache.hadoop.io.compress.zlib.ZlibFactory
				.getZlibDecompressor(conf);
			NUnit.Framework.Assert.IsNotNull("zlibDecompressor is null!", zlibDecompressor);
			NUnit.Framework.Assert.IsTrue("ZlibFactory returned unexpected inflator", zlibDecompressor
				 is org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater);
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(zlibDecompressor);
			// Now create a GZip text file.
			string tmpDir = Sharpen.Runtime.getProperty("test.build.data", "/tmp/");
			org.apache.hadoop.fs.Path f = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path
				(tmpDir), "testGzipLongOverflow.bin.gz");
			java.io.BufferedWriter bw = new java.io.BufferedWriter(new java.io.OutputStreamWriter
				(new java.util.zip.GZIPOutputStream(new java.io.FileOutputStream(f.ToString())))
				);
			int NBUF = 1024 * 4 + 1;
			char[] buf = new char[1024 * 1024];
			for (int i = 0; i < buf.Length; i++)
			{
				buf[i] = '\0';
			}
			for (int i_1 = 0; i_1 < NBUF; i_1++)
			{
				bw.write(buf);
			}
			bw.close();
			// Now read it back, using the CodecPool to establish the
			// decompressor to use.
			org.apache.hadoop.io.compress.CompressionCodecFactory ccf = new org.apache.hadoop.io.compress.CompressionCodecFactory
				(conf);
			org.apache.hadoop.io.compress.CompressionCodec codec = ccf.getCodec(f);
			org.apache.hadoop.io.compress.Decompressor decompressor = org.apache.hadoop.io.compress.CodecPool
				.getDecompressor(codec);
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			java.io.InputStream @is = fs.open(f);
			@is = codec.createInputStream(@is, decompressor);
			java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader
				(@is));
			for (int j = 0; j < NBUF; j++)
			{
				int n = br.read(buf);
				NUnit.Framework.Assert.AreEqual("got wrong read length!", n, buf.Length);
				for (int i_2 = 0; i_2 < buf.Length; i_2++)
				{
					NUnit.Framework.Assert.AreEqual("got wrong byte!", buf[i_2], '\0');
				}
			}
			br.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testGzipCodecWrite(bool useNative)
		{
			// Create a gzipped file using a compressor from the CodecPool,
			// and try to read it back via the regular GZIPInputStream.
			// Use native libs per the parameter
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY
				, useNative);
			if (useNative)
			{
				if (!org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf))
				{
					LOG.warn("testGzipCodecWrite skipped: native libs not loaded");
					return;
				}
			}
			else
			{
				NUnit.Framework.Assert.IsFalse("ZlibFactory is using native libs against request"
					, org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf));
			}
			// Ensure that the CodecPool has a BuiltInZlibDeflater in it.
			org.apache.hadoop.io.compress.Compressor zlibCompressor = org.apache.hadoop.io.compress.zlib.ZlibFactory
				.getZlibCompressor(conf);
			NUnit.Framework.Assert.IsNotNull("zlibCompressor is null!", zlibCompressor);
			NUnit.Framework.Assert.IsTrue("ZlibFactory returned unexpected deflator", useNative
				 ? zlibCompressor is org.apache.hadoop.io.compress.zlib.ZlibCompressor : zlibCompressor
				 is org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater);
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(zlibCompressor);
			// Create a GZIP text file via the Compressor interface.
			org.apache.hadoop.io.compress.CompressionCodecFactory ccf = new org.apache.hadoop.io.compress.CompressionCodecFactory
				(conf);
			org.apache.hadoop.io.compress.CompressionCodec codec = ccf.getCodec(new org.apache.hadoop.fs.Path
				("foo.gz"));
			NUnit.Framework.Assert.IsTrue("Codec for .gz file is not GzipCodec", codec is org.apache.hadoop.io.compress.GzipCodec
				);
			string msg = "This is the message we are going to compress.";
			string tmpDir = Sharpen.Runtime.getProperty("test.build.data", "/tmp/");
			string fileName = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path(tmpDir
				), "testGzipCodecWrite.txt.gz").ToString();
			java.io.BufferedWriter w = null;
			org.apache.hadoop.io.compress.Compressor gzipCompressor = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(codec);
			if (null != gzipCompressor)
			{
				// If it gives us back a Compressor, we should be able to use this
				// to write files we can then read back with Java's gzip tools.
				java.io.OutputStream os = new org.apache.hadoop.io.compress.CompressorStream(new 
					java.io.FileOutputStream(fileName), gzipCompressor);
				w = new java.io.BufferedWriter(new java.io.OutputStreamWriter(os));
				w.write(msg);
				w.close();
				org.apache.hadoop.io.compress.CodecPool.returnCompressor(gzipCompressor);
				verifyGzipFile(fileName, msg);
			}
			// Create a gzip text file via codec.getOutputStream().
			w = new java.io.BufferedWriter(new java.io.OutputStreamWriter(codec.createOutputStream
				(new java.io.FileOutputStream(fileName))));
			w.write(msg);
			w.close();
			verifyGzipFile(fileName, msg);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGzipCodecWriteJava()
		{
			testGzipCodecWrite(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGzipNativeCodecWrite()
		{
			testGzipCodecWrite(true);
		}

		public virtual void testCodecPoolAndGzipDecompressor()
		{
			// BuiltInZlibInflater should not be used as the GzipCodec decompressor.
			// Assert that this is the case.
			// Don't use native libs for this test.
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean("hadoop.native.lib", false);
			NUnit.Framework.Assert.IsFalse("ZlibFactory is using native libs against request"
				, org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf));
			// This should give us a BuiltInZlibInflater.
			org.apache.hadoop.io.compress.Decompressor zlibDecompressor = org.apache.hadoop.io.compress.zlib.ZlibFactory
				.getZlibDecompressor(conf);
			NUnit.Framework.Assert.IsNotNull("zlibDecompressor is null!", zlibDecompressor);
			NUnit.Framework.Assert.IsTrue("ZlibFactory returned unexpected inflator", zlibDecompressor
				 is org.apache.hadoop.io.compress.zlib.BuiltInZlibInflater);
			// its createOutputStream() just wraps the existing stream in a
			// java.util.zip.GZIPOutputStream.
			org.apache.hadoop.io.compress.CompressionCodecFactory ccf = new org.apache.hadoop.io.compress.CompressionCodecFactory
				(conf);
			org.apache.hadoop.io.compress.CompressionCodec codec = ccf.getCodec(new org.apache.hadoop.fs.Path
				("foo.gz"));
			NUnit.Framework.Assert.IsTrue("Codec for .gz file is not GzipCodec", codec is org.apache.hadoop.io.compress.GzipCodec
				);
			// make sure we don't get a null decompressor
			org.apache.hadoop.io.compress.Decompressor codecDecompressor = codec.createDecompressor
				();
			if (null == codecDecompressor)
			{
				NUnit.Framework.Assert.Fail("Got null codecDecompressor");
			}
			// Asking the CodecPool for a decompressor for GzipCodec
			// should not return null
			org.apache.hadoop.io.compress.Decompressor poolDecompressor = org.apache.hadoop.io.compress.CodecPool
				.getDecompressor(codec);
			if (null == poolDecompressor)
			{
				NUnit.Framework.Assert.Fail("Got null poolDecompressor");
			}
			// return a couple decompressors
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(zlibDecompressor);
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(poolDecompressor);
			org.apache.hadoop.io.compress.Decompressor poolDecompressor2 = org.apache.hadoop.io.compress.CodecPool
				.getDecompressor(codec);
			if (Sharpen.Runtime.getClassForObject(poolDecompressor) == Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor)))
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
