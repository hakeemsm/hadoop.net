using System.Collections.Generic;
using System.IO;
using System.Text;
using ICSharpCode.SharpZipLib;
using ICSharpCode.SharpZipLib.Zip.Compression;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestConcatenatedCompressedInput
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestConcatenatedCompressedInput
			).FullName);

		private static int MaxLength = 10000;

		private static JobConf defaultConf = new JobConf();

		private static FileSystem localFs = null;

		internal const string ColorRed = "[0;31m";

		internal const string ColorGreen = "[0;32m";

		internal const string ColorYellow = "[0;33;40m";

		internal const string ColorBlue = "[0;34m";

		internal const string ColorMagenta = "[0;35m";

		internal const string ColorCyan = "[0;36m";

		internal const string ColorWhite = "[0;37;40m";

		internal const string ColorBrRed = "[1;31m";

		internal const string ColorBrGreen = "[1;32m";

		internal const string ColorBrYellow = "[1;33;40m";

		internal const string ColorBrBlue = "[1;34m";

		internal const string ColorBrMagenta = "[1;35m";

		internal const string ColorBrCyan = "[1;36m";

		internal const string ColorBrWhite = "[1;37;40m";

		internal const string ColorNormal = "[0m";

		static TestConcatenatedCompressedInput()
		{
			// from ~roelofs/ss30b-colors.hh
			// background doesn't matter...  "[0m"
			// background doesn't matter...  "[0m"
			// DO force black background     "[0m"
			// do NOT force black background "[0m"
			// background doesn't matter...  "[0m"
			// background doesn't matter...  "[0m"
			// DO force black background     "[0m"
			// background doesn't matter...  "[0m"
			// background doesn't matter...  "[0m"
			// DO force black background     "[0m"
			// do NOT force black background "[0m"
			// background doesn't matter...  "[0m"
			// background doesn't matter...  "[0m"
			// DO force black background     "[0m"
			try
			{
				defaultConf.Set("fs.defaultFS", "file:///");
				localFs = FileSystem.GetLocal(defaultConf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "/tmp")), "TestConcatenatedCompressedInput").MakeQualified(localFs);

		/// <exception cref="System.IO.IOException"/>
		private static LineReader MakeStream(string str)
		{
			return new LineReader(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(
				str, "UTF-8")), defaultConf);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem fs, Path name, CompressionCodec codec, string
			 contents)
		{
			OutputStream stm;
			if (codec == null)
			{
				stm = fs.Create(name);
			}
			else
			{
				stm = codec.CreateOutputStream(fs.Create(name));
			}
			stm.Write(Sharpen.Runtime.GetBytesForString(contents));
			stm.Close();
		}

		private static readonly Reporter voidReporter = Reporter.Null;

		/// <exception cref="System.IO.IOException"/>
		private static IList<Text> ReadSplit(TextInputFormat format, InputSplit split, JobConf
			 jobConf)
		{
			IList<Text> result = new AList<Text>();
			RecordReader<LongWritable, Text> reader = format.GetRecordReader(split, jobConf, 
				voidReporter);
			LongWritable key = reader.CreateKey();
			Text value = reader.CreateValue();
			while (reader.Next(key, value))
			{
				result.AddItem(value);
				value = reader.CreateValue();
			}
			reader.Close();
			return result;
		}

		/// <summary>Test using Hadoop's original, native-zlib gzip codec for reading.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGzip()
		{
			JobConf jobConf = new JobConf(defaultConf);
			CompressionCodec gzip = new GzipCodec();
			ReflectionUtils.SetConf(gzip, jobConf);
			localFs.Delete(workDir, true);
			// preferred, but not compatible with Apache/trunk instance of Hudson:
			/*
			assertFalse("[native (C/C++) codec]",
			(org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor.class ==
			gzip.getDecompressorType()) );
			System.out.println(COLOR_BR_RED +
			"testGzip() using native-zlib Decompressor (" +
			gzip.getDecompressorType() + ")" + COLOR_NORMAL);
			*/
			// alternative:
			if (typeof(BuiltInGzipDecompressor) == gzip.GetDecompressorType())
			{
				System.Console.Out.WriteLine(ColorBrRed + "testGzip() using native-zlib Decompressor ("
					 + gzip.GetDecompressorType() + ")" + ColorNormal);
			}
			else
			{
				Log.Warn("testGzip() skipped:  native (C/C++) libs not loaded");
				return;
			}
			/*
			*      // THIS IS BUGGY: omits 2nd/3rd gzip headers; screws up 2nd/3rd CRCs--
			*      //                see https://issues.apache.org/jira/browse/HADOOP-6799
			*  Path fnHDFS = new Path(workDir, "concat" + gzip.getDefaultExtension());
			*  //OutputStream out = localFs.create(fnHDFS);
			*  //GzipCodec.GzipOutputStream gzOStm = new GzipCodec.GzipOutputStream(out);
			*      // can just combine those two lines, probably
			*  //GzipCodec.GzipOutputStream gzOStm =
			*  //  new GzipCodec.GzipOutputStream(localFs.create(fnHDFS));
			*      // oops, no:  this is a protected helper class; need to access
			*      //   it via createOutputStream() instead:
			*  OutputStream out = localFs.create(fnHDFS);
			*  Compressor gzCmp = gzip.createCompressor();
			*  CompressionOutputStream gzOStm = gzip.createOutputStream(out, gzCmp);
			*      // this SHOULD be going to HDFS:  got out from localFs == HDFS
			*      //   ...yup, works
			*  gzOStm.write("first gzip concat\n member\nwith three lines\n".getBytes());
			*  gzOStm.finish();
			*  gzOStm.resetState();
			*  gzOStm.write("2nd gzip concat member\n".getBytes());
			*  gzOStm.finish();
			*  gzOStm.resetState();
			*  gzOStm.write("gzip concat\nmember #3\n".getBytes());
			*  gzOStm.close();
			*      //
			*  String fn = "hdfs-to-local-concat" + gzip.getDefaultExtension();
			*  Path fnLocal = new Path(System.getProperty("test.concat.data","/tmp"), fn);
			*  localFs.copyToLocalFile(fnHDFS, fnLocal);
			*/
			// copy prebuilt (correct!) version of concat.gz to HDFS
			string fn = "concat" + gzip.GetDefaultExtension();
			Path fnLocal = new Path(Runtime.GetProperty("test.concat.data", "/tmp"), fn);
			Path fnHDFS = new Path(workDir, fn);
			localFs.CopyFromLocalFile(fnLocal, fnHDFS);
			WriteFile(localFs, new Path(workDir, "part2.txt.gz"), gzip, "this is a test\nof gzip\n"
				);
			FileInputFormat.SetInputPaths(jobConf, workDir);
			TextInputFormat format = new TextInputFormat();
			format.Configure(jobConf);
			InputSplit[] splits = format.GetSplits(jobConf, 100);
			NUnit.Framework.Assert.AreEqual("compressed splits == 2", 2, splits.Length);
			FileSplit tmp = (FileSplit)splits[0];
			if (tmp.GetPath().GetName().Equals("part2.txt.gz"))
			{
				splits[0] = splits[1];
				splits[1] = tmp;
			}
			IList<Text> results = ReadSplit(format, splits[0], jobConf);
			NUnit.Framework.Assert.AreEqual("splits[0] num lines", 6, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[0][5]", "member #3", results[5].ToString(
				));
			results = ReadSplit(format, splits[1], jobConf);
			NUnit.Framework.Assert.AreEqual("splits[1] num lines", 2, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[1][0]", "this is a test", results[0].ToString
				());
			NUnit.Framework.Assert.AreEqual("splits[1][1]", "of gzip", results[1].ToString());
		}

		/// <summary>Test using the raw Inflater codec for reading gzip files.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPrototypeInflaterGzip()
		{
			CompressionCodec gzip = new GzipCodec();
			// used only for file extension
			localFs.Delete(workDir, true);
			// localFs = FileSystem instance
			System.Console.Out.WriteLine(ColorBrBlue + "testPrototypeInflaterGzip() using " +
				 "non-native/Java Inflater and manual gzip header/trailer parsing" + ColorNormal
				);
			// copy prebuilt (correct!) version of concat.gz to HDFS
			string fn = "concat" + gzip.GetDefaultExtension();
			Path fnLocal = new Path(Runtime.GetProperty("test.concat.data", "/tmp"), fn);
			Path fnHDFS = new Path(workDir, fn);
			localFs.CopyFromLocalFile(fnLocal, fnHDFS);
			FileInputStream @in = new FileInputStream(fnLocal.ToString());
			NUnit.Framework.Assert.AreEqual("concat bytes available", 148, @in.Available());
			// should wrap all of this header-reading stuff in a running-CRC wrapper
			// (did so in BuiltInGzipDecompressor; see below)
			byte[] compressedBuf = new byte[256];
			int numBytesRead = @in.Read(compressedBuf, 0, 10);
			NUnit.Framework.Assert.AreEqual("header bytes read", 10, numBytesRead);
			NUnit.Framework.Assert.AreEqual("1st byte", unchecked((int)(0x1f)), compressedBuf
				[0] & unchecked((int)(0xff)));
			NUnit.Framework.Assert.AreEqual("2nd byte", unchecked((int)(0x8b)), compressedBuf
				[1] & unchecked((int)(0xff)));
			NUnit.Framework.Assert.AreEqual("3rd byte (compression method)", 8, compressedBuf
				[2] & unchecked((int)(0xff)));
			byte flags = unchecked((byte)(compressedBuf[3] & unchecked((int)(0xff))));
			if ((flags & unchecked((int)(0x04))) != 0)
			{
				// FEXTRA
				numBytesRead = @in.Read(compressedBuf, 0, 2);
				NUnit.Framework.Assert.AreEqual("XLEN bytes read", 2, numBytesRead);
				int xlen = ((compressedBuf[1] << 8) | compressedBuf[0]) & unchecked((int)(0xffff)
					);
				@in.Skip(xlen);
			}
			if ((flags & unchecked((int)(0x08))) != 0)
			{
				// FNAME
				while ((numBytesRead = @in.Read()) != 0)
				{
					NUnit.Framework.Assert.IsFalse("unexpected end-of-file while reading filename", numBytesRead
						 == -1);
				}
			}
			if ((flags & unchecked((int)(0x10))) != 0)
			{
				// FCOMMENT
				while ((numBytesRead = @in.Read()) != 0)
				{
					NUnit.Framework.Assert.IsFalse("unexpected end-of-file while reading comment", numBytesRead
						 == -1);
				}
			}
			if ((flags & unchecked((int)(0xe0))) != 0)
			{
				// reserved
				NUnit.Framework.Assert.IsTrue("reserved bits are set??", (flags & unchecked((int)
					(0xe0))) == 0);
			}
			if ((flags & unchecked((int)(0x02))) != 0)
			{
				// FHCRC
				numBytesRead = @in.Read(compressedBuf, 0, 2);
				NUnit.Framework.Assert.AreEqual("CRC16 bytes read", 2, numBytesRead);
				int crc16 = ((compressedBuf[1] << 8) | compressedBuf[0]) & unchecked((int)(0xffff
					));
			}
			// ready to go!  next bytes should be start of deflated stream, suitable
			// for Inflater
			numBytesRead = @in.Read(compressedBuf);
			// Inflater docs refer to a "dummy byte":  no clue what that's about;
			// appears to work fine without one
			byte[] uncompressedBuf = new byte[256];
			Inflater inflater = new Inflater(true);
			inflater.SetInput(compressedBuf, 0, numBytesRead);
			try
			{
				int numBytesUncompressed = inflater.Inflate(uncompressedBuf);
				string outString = Sharpen.Runtime.GetStringForBytes(uncompressedBuf, 0, numBytesUncompressed
					, "UTF-8");
				System.Console.Out.WriteLine("uncompressed data of first gzip member = [" + outString
					 + "]");
			}
			catch (SharpZipBaseException ex)
			{
				throw new IOException(ex.Message);
			}
			@in.Close();
		}

		/// <summary>Test using the new BuiltInGzipDecompressor codec for reading gzip files.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBuiltInGzipDecompressor()
		{
			// NOTE:  This fails on RHEL4 with "java.io.IOException: header crc mismatch"
			//        due to buggy version of zlib (1.2.1.2) included.
			JobConf jobConf = new JobConf(defaultConf);
			jobConf.SetBoolean("io.native.lib.available", false);
			CompressionCodec gzip = new GzipCodec();
			ReflectionUtils.SetConf(gzip, jobConf);
			localFs.Delete(workDir, true);
			NUnit.Framework.Assert.AreEqual("[non-native (Java) codec]", typeof(BuiltInGzipDecompressor
				), gzip.GetDecompressorType());
			System.Console.Out.WriteLine(ColorBrYellow + "testBuiltInGzipDecompressor() using"
				 + " non-native (Java Inflater) Decompressor (" + gzip.GetDecompressorType() + ")"
				 + ColorNormal);
			// copy single-member test file to HDFS
			string fn1 = "testConcatThenCompress.txt" + gzip.GetDefaultExtension();
			Path fnLocal1 = new Path(Runtime.GetProperty("test.concat.data", "/tmp"), fn1);
			Path fnHDFS1 = new Path(workDir, fn1);
			localFs.CopyFromLocalFile(fnLocal1, fnHDFS1);
			// copy multiple-member test file to HDFS
			// (actually in "seekable gzip" format, a la JIRA PIG-42)
			string fn2 = "testCompressThenConcat.txt" + gzip.GetDefaultExtension();
			Path fnLocal2 = new Path(Runtime.GetProperty("test.concat.data", "/tmp"), fn2);
			Path fnHDFS2 = new Path(workDir, fn2);
			localFs.CopyFromLocalFile(fnLocal2, fnHDFS2);
			FileInputFormat.SetInputPaths(jobConf, workDir);
			// here's first pair of DecompressorStreams:
			FileInputStream in1 = new FileInputStream(fnLocal1.ToString());
			FileInputStream in2 = new FileInputStream(fnLocal2.ToString());
			NUnit.Framework.Assert.AreEqual("concat bytes available", 2734, in1.Available());
			NUnit.Framework.Assert.AreEqual("concat bytes available", 3413, in2.Available());
			// w/hdr CRC
			CompressionInputStream cin2 = gzip.CreateInputStream(in2);
			LineReader @in = new LineReader(cin2);
			Text @out = new Text();
			int numBytes;
			int totalBytes = 0;
			int lineNum = 0;
			while ((numBytes = @in.ReadLine(@out)) > 0)
			{
				++lineNum;
				totalBytes += numBytes;
			}
			@in.Close();
			NUnit.Framework.Assert.AreEqual("total uncompressed bytes in concatenated test file"
				, 5346, totalBytes);
			NUnit.Framework.Assert.AreEqual("total uncompressed lines in concatenated test file"
				, 84, lineNum);
			// test BuiltInGzipDecompressor with lots of different input-buffer sizes
			DoMultipleGzipBufferSizes(jobConf, false);
			// test GzipZlibDecompressor (native), just to be sure
			// (FIXME?  could move this call to testGzip(), but would need filename
			// setup above) (alternatively, maybe just nuke testGzip() and extend this?)
			DoMultipleGzipBufferSizes(jobConf, true);
		}

		// this tests either the native or the non-native gzip decoder with 43
		// input-buffer sizes in order to try to catch any parser/state-machine
		// errors at buffer boundaries
		/// <exception cref="System.IO.IOException"/>
		private static void DoMultipleGzipBufferSizes(JobConf jConf, bool useNative)
		{
			System.Console.Out.WriteLine(ColorYellow + "doMultipleGzipBufferSizes() using " +
				 (useNative ? "GzipZlibDecompressor" : "BuiltInGzipDecompressor") + ColorNormal);
			jConf.SetBoolean("io.native.lib.available", useNative);
			int bufferSize;
			// ideally would add some offsets/shifts in here (e.g., via extra fields
			// of various sizes), but...significant work to hand-generate each header
			for (bufferSize = 1; bufferSize < 34; ++bufferSize)
			{
				jConf.SetInt("io.file.buffer.size", bufferSize);
				DoSingleGzipBufferSize(jConf);
			}
			bufferSize = 512;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 2 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 4 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 63 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 64 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 65 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 127 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 128 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
			bufferSize = 129 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleGzipBufferSize(jConf);
		}

		// this tests both files (testCompressThenConcat, testConcatThenCompress);
		// all should work with either native zlib or new Inflater-based decoder
		/// <exception cref="System.IO.IOException"/>
		private static void DoSingleGzipBufferSize(JobConf jConf)
		{
			TextInputFormat format = new TextInputFormat();
			format.Configure(jConf);
			// here's Nth pair of DecompressorStreams:
			InputSplit[] splits = format.GetSplits(jConf, 100);
			NUnit.Framework.Assert.AreEqual("compressed splits == 2", 2, splits.Length);
			FileSplit tmp = (FileSplit)splits[0];
			if (tmp.GetPath().GetName().Equals("testCompressThenConcat.txt.gz"))
			{
				System.Console.Out.WriteLine("  (swapping)");
				splits[0] = splits[1];
				splits[1] = tmp;
			}
			IList<Text> results = ReadSplit(format, splits[0], jConf);
			NUnit.Framework.Assert.AreEqual("splits[0] length (num lines)", 84, results.Count
				);
			NUnit.Framework.Assert.AreEqual("splits[0][0]", "Call me Ishmael. Some years ago--never mind how long precisely--having"
				, results[0].ToString());
			NUnit.Framework.Assert.AreEqual("splits[0][42]", "Tell me, does the magnetic virtue of the needles of the compasses of"
				, results[42].ToString());
			results = ReadSplit(format, splits[1], jConf);
			NUnit.Framework.Assert.AreEqual("splits[1] length (num lines)", 84, results.Count
				);
			NUnit.Framework.Assert.AreEqual("splits[1][0]", "Call me Ishmael. Some years ago--never mind how long precisely--having"
				, results[0].ToString());
			NUnit.Framework.Assert.AreEqual("splits[1][42]", "Tell me, does the magnetic virtue of the needles of the compasses of"
				, results[42].ToString());
		}

		/// <summary>Test using the bzip2 codec for reading</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBzip2()
		{
			JobConf jobConf = new JobConf(defaultConf);
			CompressionCodec bzip2 = new BZip2Codec();
			ReflectionUtils.SetConf(bzip2, jobConf);
			localFs.Delete(workDir, true);
			System.Console.Out.WriteLine(ColorBrCyan + "testBzip2() using non-native CBZip2InputStream (presumably)"
				 + ColorNormal);
			// copy prebuilt (correct!) version of concat.bz2 to HDFS
			string fn = "concat" + bzip2.GetDefaultExtension();
			Path fnLocal = new Path(Runtime.GetProperty("test.concat.data", "/tmp"), fn);
			Path fnHDFS = new Path(workDir, fn);
			localFs.CopyFromLocalFile(fnLocal, fnHDFS);
			WriteFile(localFs, new Path(workDir, "part2.txt.bz2"), bzip2, "this is a test\nof bzip2\n"
				);
			FileInputFormat.SetInputPaths(jobConf, workDir);
			TextInputFormat format = new TextInputFormat();
			// extends FileInputFormat
			format.Configure(jobConf);
			format.SetMinSplitSize(256);
			// work around 2-byte splits issue
			// [135 splits for a 208-byte file and a 62-byte file(!)]
			InputSplit[] splits = format.GetSplits(jobConf, 100);
			NUnit.Framework.Assert.AreEqual("compressed splits == 2", 2, splits.Length);
			FileSplit tmp = (FileSplit)splits[0];
			if (tmp.GetPath().GetName().Equals("part2.txt.bz2"))
			{
				splits[0] = splits[1];
				splits[1] = tmp;
			}
			IList<Text> results = ReadSplit(format, splits[0], jobConf);
			NUnit.Framework.Assert.AreEqual("splits[0] num lines", 6, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[0][5]", "member #3", results[5].ToString(
				));
			results = ReadSplit(format, splits[1], jobConf);
			NUnit.Framework.Assert.AreEqual("splits[1] num lines", 2, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[1][0]", "this is a test", results[0].ToString
				());
			NUnit.Framework.Assert.AreEqual("splits[1][1]", "of bzip2", results[1].ToString()
				);
		}

		/// <summary>Extended bzip2 test, similar to BuiltInGzipDecompressor test above.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMoreBzip2()
		{
			JobConf jobConf = new JobConf(defaultConf);
			CompressionCodec bzip2 = new BZip2Codec();
			ReflectionUtils.SetConf(bzip2, jobConf);
			localFs.Delete(workDir, true);
			System.Console.Out.WriteLine(ColorBrMagenta + "testMoreBzip2() using non-native CBZip2InputStream (presumably)"
				 + ColorNormal);
			// copy single-member test file to HDFS
			string fn1 = "testConcatThenCompress.txt" + bzip2.GetDefaultExtension();
			Path fnLocal1 = new Path(Runtime.GetProperty("test.concat.data", "/tmp"), fn1);
			Path fnHDFS1 = new Path(workDir, fn1);
			localFs.CopyFromLocalFile(fnLocal1, fnHDFS1);
			// copy multiple-member test file to HDFS
			string fn2 = "testCompressThenConcat.txt" + bzip2.GetDefaultExtension();
			Path fnLocal2 = new Path(Runtime.GetProperty("test.concat.data", "/tmp"), fn2);
			Path fnHDFS2 = new Path(workDir, fn2);
			localFs.CopyFromLocalFile(fnLocal2, fnHDFS2);
			FileInputFormat.SetInputPaths(jobConf, workDir);
			// here's first pair of BlockDecompressorStreams:
			FileInputStream in1 = new FileInputStream(fnLocal1.ToString());
			FileInputStream in2 = new FileInputStream(fnLocal2.ToString());
			NUnit.Framework.Assert.AreEqual("concat bytes available", 2567, in1.Available());
			NUnit.Framework.Assert.AreEqual("concat bytes available", 3056, in2.Available());
			/*
			// FIXME
			// The while-loop below dies at the beginning of the 2nd concatenated
			// member (after 17 lines successfully read) with:
			//
			//   java.io.IOException: bad block header
			//   at org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.initBlock(
			//   CBZip2InputStream.java:527)
			//
			// It is not critical to concatenated-gzip support, HADOOP-6835, so it's
			// simply commented out for now (and HADOOP-6852 filed).  If and when the
			// latter issue is resolved--perhaps by fixing an error here--this code
			// should be reenabled.  Note that the doMultipleBzip2BufferSizes() test
			// below uses the same testCompressThenConcat.txt.bz2 file but works fine.
			
			CompressionInputStream cin2 = bzip2.createInputStream(in2);
			LineReader in = new LineReader(cin2);
			Text out = new Text();
			
			int numBytes, totalBytes=0, lineNum=0;
			while ((numBytes = in.readLine(out)) > 0) {
			++lineNum;
			totalBytes += numBytes;
			}
			in.close();
			assertEquals("total uncompressed bytes in concatenated test file",
			5346, totalBytes);
			assertEquals("total uncompressed lines in concatenated test file",
			84, lineNum);
			*/
			// test CBZip2InputStream with lots of different input-buffer sizes
			DoMultipleBzip2BufferSizes(jobConf, false);
		}

		// no native version of bzip2 codec (yet?)
		//doMultipleBzip2BufferSizes(jobConf, true);
		// this tests either the native or the non-native gzip decoder with more than
		// three dozen input-buffer sizes in order to try to catch any parser/state-
		// machine errors at buffer boundaries
		/// <exception cref="System.IO.IOException"/>
		private static void DoMultipleBzip2BufferSizes(JobConf jConf, bool useNative)
		{
			System.Console.Out.WriteLine(ColorMagenta + "doMultipleBzip2BufferSizes() using "
				 + "default bzip2 decompressor" + ColorNormal);
			jConf.SetBoolean("io.native.lib.available", useNative);
			int bufferSize;
			// ideally would add some offsets/shifts in here (e.g., via extra header
			// data?), but...significant work to hand-generate each header, and no
			// bzip2 spec for reference
			for (bufferSize = 1; bufferSize < 34; ++bufferSize)
			{
				jConf.SetInt("io.file.buffer.size", bufferSize);
				DoSingleBzip2BufferSize(jConf);
			}
			bufferSize = 512;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 2 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 4 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 63 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 64 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 65 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 127 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 128 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
			bufferSize = 129 * 1024;
			jConf.SetInt("io.file.buffer.size", bufferSize);
			DoSingleBzip2BufferSize(jConf);
		}

		// this tests both files (testCompressThenConcat, testConcatThenCompress); all
		// should work with existing Java bzip2 decoder and any future native version
		/// <exception cref="System.IO.IOException"/>
		private static void DoSingleBzip2BufferSize(JobConf jConf)
		{
			TextInputFormat format = new TextInputFormat();
			format.Configure(jConf);
			format.SetMinSplitSize(5500);
			// work around 256-byte/22-splits issue
			// here's Nth pair of DecompressorStreams:
			InputSplit[] splits = format.GetSplits(jConf, 100);
			NUnit.Framework.Assert.AreEqual("compressed splits == 2", 2, splits.Length);
			FileSplit tmp = (FileSplit)splits[0];
			if (tmp.GetPath().GetName().Equals("testCompressThenConcat.txt.gz"))
			{
				System.Console.Out.WriteLine("  (swapping)");
				splits[0] = splits[1];
				splits[1] = tmp;
			}
			// testConcatThenCompress (single)
			IList<Text> results = ReadSplit(format, splits[0], jConf);
			NUnit.Framework.Assert.AreEqual("splits[0] length (num lines)", 84, results.Count
				);
			NUnit.Framework.Assert.AreEqual("splits[0][0]", "Call me Ishmael. Some years ago--never mind how long precisely--having"
				, results[0].ToString());
			NUnit.Framework.Assert.AreEqual("splits[0][42]", "Tell me, does the magnetic virtue of the needles of the compasses of"
				, results[42].ToString());
			// testCompressThenConcat (multi)
			results = ReadSplit(format, splits[1], jConf);
			NUnit.Framework.Assert.AreEqual("splits[1] length (num lines)", 84, results.Count
				);
			NUnit.Framework.Assert.AreEqual("splits[1][0]", "Call me Ishmael. Some years ago--never mind how long precisely--having"
				, results[0].ToString());
			NUnit.Framework.Assert.AreEqual("splits[1][42]", "Tell me, does the magnetic virtue of the needles of the compasses of"
				, results[42].ToString());
		}

		private static string Unquote(string @in)
		{
			StringBuilder result = new StringBuilder();
			for (int i = 0; i < @in.Length; ++i)
			{
				char ch = @in[i];
				if (ch == '\\')
				{
					ch = @in[++i];
					switch (ch)
					{
						case 'n':
						{
							result.Append('\n');
							break;
						}

						case 'r':
						{
							result.Append('\r');
							break;
						}

						default:
						{
							result.Append(ch);
							break;
						}
					}
				}
				else
				{
					result.Append(ch);
				}
			}
			return result.ToString();
		}

		/// <summary>Parse the command line arguments into lines and display the result.</summary>
		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			foreach (string arg in args)
			{
				System.Console.Out.WriteLine("Working on " + arg);
				LineReader reader = MakeStream(Unquote(arg));
				Org.Apache.Hadoop.IO.Text line = new Org.Apache.Hadoop.IO.Text();
				int size = reader.ReadLine(line);
				while (size > 0)
				{
					System.Console.Out.WriteLine("Got: " + line.ToString());
					size = reader.ReadLine(line);
				}
				reader.Close();
			}
		}
	}
}
