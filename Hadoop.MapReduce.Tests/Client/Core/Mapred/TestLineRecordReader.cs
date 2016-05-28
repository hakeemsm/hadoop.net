using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Compress.Compressors.Bzip2;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestLineRecordReader
	{
		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "target"), "data"), "TestTextInputFormat");

		private static Path inputDir = new Path(workDir, "input");

		/// <exception cref="System.IO.IOException"/>
		private void TestSplitRecords(string testFileName, long firstSplitLength)
		{
			Uri testFileUrl = GetType().GetClassLoader().GetResource(testFileName);
			NUnit.Framework.Assert.IsNotNull("Cannot find " + testFileName, testFileUrl);
			FilePath testFile = new FilePath(testFileUrl.GetFile());
			long testFileSize = testFile.Length();
			Path testFilePath = new Path(testFile.GetAbsolutePath());
			Configuration conf = new Configuration();
			TestSplitRecordsForFile(conf, firstSplitLength, testFileSize, testFilePath);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestSplitRecordsForFile(Configuration conf, long firstSplitLength, long
			 testFileSize, Path testFilePath)
		{
			conf.SetInt(LineRecordReader.MaxLineLength, int.MaxValue);
			NUnit.Framework.Assert.IsTrue("unexpected test data at " + testFilePath, testFileSize
				 > firstSplitLength);
			string delimiter = conf.Get("textinputformat.record.delimiter");
			byte[] recordDelimiterBytes = null;
			if (null != delimiter)
			{
				recordDelimiterBytes = Sharpen.Runtime.GetBytesForString(delimiter, Charsets.Utf8
					);
			}
			// read the data without splitting to count the records
			FileSplit split = new FileSplit(testFilePath, 0, testFileSize, (string[])null);
			LineRecordReader reader = new LineRecordReader(conf, split, recordDelimiterBytes);
			LongWritable key = new LongWritable();
			Text value = new Text();
			int numRecordsNoSplits = 0;
			while (reader.Next(key, value))
			{
				++numRecordsNoSplits;
			}
			reader.Close();
			// count the records in the first split
			split = new FileSplit(testFilePath, 0, firstSplitLength, (string[])null);
			reader = new LineRecordReader(conf, split, recordDelimiterBytes);
			int numRecordsFirstSplit = 0;
			while (reader.Next(key, value))
			{
				++numRecordsFirstSplit;
			}
			reader.Close();
			// count the records in the second split
			split = new FileSplit(testFilePath, firstSplitLength, testFileSize - firstSplitLength
				, (string[])null);
			reader = new LineRecordReader(conf, split, recordDelimiterBytes);
			int numRecordsRemainingSplits = 0;
			while (reader.Next(key, value))
			{
				++numRecordsRemainingSplits;
			}
			reader.Close();
			NUnit.Framework.Assert.AreEqual("Unexpected number of records in split", numRecordsNoSplits
				, numRecordsFirstSplit + numRecordsRemainingSplits);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBzip2SplitEndsAtCR()
		{
			// the test data contains a carriage-return at the end of the first
			// split which ends at compressed offset 136498 and the next
			// character is not a linefeed
			TestSplitRecords("blockEndingInCR.txt.bz2", 136498);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBzip2SplitEndsAtCRThenLF()
		{
			// the test data contains a carriage-return at the end of the first
			// split which ends at compressed offset 136498 and the next
			// character is a linefeed
			TestSplitRecords("blockEndingInCRThenLF.txt.bz2", 136498);
		}

		//This test ensures record reader doesn't lose records when it starts
		//exactly at the starting byte of a bz2 compressed block
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBzip2SplitStartAtBlockMarker()
		{
			//136504 in blockEndingInCR.txt.bz2 is the byte at which the bz2 block ends
			//In the following test cases record readers should iterate over all the records
			//and should not miss any record.
			//Start next split at just the start of the block.
			TestSplitRecords("blockEndingInCR.txt.bz2", 136504);
			//Start next split a byte forward in next block.
			TestSplitRecords("blockEndingInCR.txt.bz2", 136505);
			//Start next split 3 bytes forward in next block.
			TestSplitRecords("blockEndingInCR.txt.bz2", 136508);
			//Start next split 10 bytes from behind the end marker.
			TestSplitRecords("blockEndingInCR.txt.bz2", 136494);
		}

		// Use the LineRecordReader to read records from the file
		/// <exception cref="System.IO.IOException"/>
		public virtual AList<string> ReadRecords(Uri testFileUrl, int splitSize)
		{
			// Set up context
			FilePath testFile = new FilePath(testFileUrl.GetFile());
			long testFileSize = testFile.Length();
			Path testFilePath = new Path(testFile.GetAbsolutePath());
			Configuration conf = new Configuration();
			conf.SetInt("io.file.buffer.size", 1);
			// Gather the records returned by the record reader
			AList<string> records = new AList<string>();
			long offset = 0;
			LongWritable key = new LongWritable();
			Text value = new Text();
			while (offset < testFileSize)
			{
				FileSplit split = new FileSplit(testFilePath, offset, splitSize, (string[])null);
				LineRecordReader reader = new LineRecordReader(conf, split);
				while (reader.Next(key, value))
				{
					records.AddItem(value.ToString());
				}
				offset += splitSize;
			}
			return records;
		}

		// Gather the records by just splitting on new lines
		/// <exception cref="System.IO.IOException"/>
		public virtual string[] ReadRecordsDirectly(Uri testFileUrl, bool bzip)
		{
			int MaxDataSize = 1024 * 1024;
			byte[] data = new byte[MaxDataSize];
			FileInputStream fis = new FileInputStream(testFileUrl.GetFile());
			int count;
			if (bzip)
			{
				BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(fis);
				count = bzIn.Read(data);
				bzIn.Close();
			}
			else
			{
				count = fis.Read(data);
			}
			fis.Close();
			NUnit.Framework.Assert.IsTrue("Test file data too big for buffer", count < data.Length
				);
			return Sharpen.Runtime.GetStringForBytes(data, 0, count, "UTF-8").Split("\n");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckRecordSpanningMultipleSplits(string testFile, int splitSize
			, bool bzip)
		{
			Uri testFileUrl = GetType().GetClassLoader().GetResource(testFile);
			AList<string> records = ReadRecords(testFileUrl, splitSize);
			string[] actuals = ReadRecordsDirectly(testFileUrl, bzip);
			NUnit.Framework.Assert.AreEqual("Wrong number of records", actuals.Length, records
				.Count);
			bool hasLargeRecord = false;
			for (int i = 0; i < actuals.Length; ++i)
			{
				NUnit.Framework.Assert.AreEqual(actuals[i], records[i]);
				if (actuals[i].Length > 2 * splitSize)
				{
					hasLargeRecord = true;
				}
			}
			NUnit.Framework.Assert.IsTrue("Invalid test data. Doesn't have a large enough record"
				, hasLargeRecord);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecordSpanningMultipleSplits()
		{
			CheckRecordSpanningMultipleSplits("recordSpanningMultipleSplits.txt", 10, false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecordSpanningMultipleSplitsCompressed()
		{
			// The file is generated with bz2 block size of 100k. The split size
			// needs to be larger than that for the CompressedSplitLineReader to
			// work.
			CheckRecordSpanningMultipleSplits("recordSpanningMultipleSplits.txt.bz2", 200 * 1000
				, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStripBOM()
		{
			// the test data contains a BOM at the start of the file
			// confirm the BOM is skipped by LineRecordReader
			string Utf8Bom = "\uFEFF";
			Uri testFileUrl = GetType().GetClassLoader().GetResource("testBOM.txt");
			NUnit.Framework.Assert.IsNotNull("Cannot find testBOM.txt", testFileUrl);
			FilePath testFile = new FilePath(testFileUrl.GetFile());
			Path testFilePath = new Path(testFile.GetAbsolutePath());
			long testFileSize = testFile.Length();
			Configuration conf = new Configuration();
			conf.SetInt(LineRecordReader.MaxLineLength, int.MaxValue);
			// read the data and check whether BOM is skipped
			FileSplit split = new FileSplit(testFilePath, 0, testFileSize, (string[])null);
			LineRecordReader reader = new LineRecordReader(conf, split);
			LongWritable key = new LongWritable();
			Text value = new Text();
			int numRecords = 0;
			bool firstLine = true;
			bool skipBOM = true;
			while (reader.Next(key, value))
			{
				if (firstLine)
				{
					firstLine = false;
					if (value.ToString().StartsWith(Utf8Bom))
					{
						skipBOM = false;
					}
				}
				++numRecords;
			}
			reader.Close();
			NUnit.Framework.Assert.IsTrue("BOM is not skipped", skipBOM);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleClose()
		{
			Uri testFileUrl = GetType().GetClassLoader().GetResource("recordSpanningMultipleSplits.txt.bz2"
				);
			NUnit.Framework.Assert.IsNotNull("Cannot find recordSpanningMultipleSplits.txt.bz2"
				, testFileUrl);
			FilePath testFile = new FilePath(testFileUrl.GetFile());
			Path testFilePath = new Path(testFile.GetAbsolutePath());
			long testFileSize = testFile.Length();
			Configuration conf = new Configuration();
			conf.SetInt(LineRecordReader.MaxLineLength, int.MaxValue);
			FileSplit split = new FileSplit(testFilePath, 0, testFileSize, (string[])null);
			LineRecordReader reader = new LineRecordReader(conf, split);
			LongWritable key = new LongWritable();
			Text value = new Text();
			//noinspection StatementWithEmptyBody
			while (reader.Next(key, value))
			{
			}
			reader.Close();
			reader.Close();
			BZip2Codec codec = new BZip2Codec();
			codec.SetConf(conf);
			ICollection<Decompressor> decompressors = new HashSet<Decompressor>();
			for (int i = 0; i < 10; ++i)
			{
				decompressors.AddItem(CodecPool.GetDecompressor(codec));
			}
			NUnit.Framework.Assert.AreEqual(10, decompressors.Count);
		}

		/// <summary>Writes the input test file</summary>
		/// <param name="conf"/>
		/// <returns>Path of the file created</returns>
		/// <exception cref="System.IO.IOException"/>
		private Path CreateInputFile(Configuration conf, string data)
		{
			FileSystem localFs = FileSystem.GetLocal(conf);
			Path file = new Path(inputDir, "test.txt");
			TextWriter writer = new OutputStreamWriter(localFs.Create(file));
			try
			{
				writer.Write(data);
			}
			finally
			{
				writer.Close();
			}
			return file;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUncompressedInput()
		{
			Configuration conf = new Configuration();
			// single char delimiter, best case
			string inputData = "abc+def+ghi+jkl+mno+pqr+stu+vw +xyz";
			Path inputFile = CreateInputFile(conf, inputData);
			conf.Set("textinputformat.record.delimiter", "+");
			for (int bufferSize = 1; bufferSize <= inputData.Length; bufferSize++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize);
					TestSplitRecordsForFile(conf, splitSize, inputData.Length, inputFile);
				}
			}
			// multi char delimiter, best case
			inputData = "abc|+|def|+|ghi|+|jkl|+|mno|+|pqr|+|stu|+|vw |+|xyz";
			inputFile = CreateInputFile(conf, inputData);
			conf.Set("textinputformat.record.delimiter", "|+|");
			for (int bufferSize_1 = 1; bufferSize_1 <= inputData.Length; bufferSize_1++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize_1);
					TestSplitRecordsForFile(conf, splitSize, inputData.Length, inputFile);
				}
			}
			// single char delimiter with empty records
			inputData = "abc+def++ghi+jkl++mno+pqr++stu+vw ++xyz";
			inputFile = CreateInputFile(conf, inputData);
			conf.Set("textinputformat.record.delimiter", "+");
			for (int bufferSize_2 = 1; bufferSize_2 <= inputData.Length; bufferSize_2++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize_2);
					TestSplitRecordsForFile(conf, splitSize, inputData.Length, inputFile);
				}
			}
			// multi char delimiter with empty records
			inputData = "abc|+||+|defghi|+|jkl|+||+|mno|+|pqr|+||+|stu|+|vw |+||+|xyz";
			inputFile = CreateInputFile(conf, inputData);
			conf.Set("textinputformat.record.delimiter", "|+|");
			for (int bufferSize_3 = 1; bufferSize_3 <= inputData.Length; bufferSize_3++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize_3);
					TestSplitRecordsForFile(conf, splitSize, inputData.Length, inputFile);
				}
			}
			// multi char delimiter with starting part of the delimiter in the data
			inputData = "abc+def+-ghi+jkl+-mno+pqr+-stu+vw +-xyz";
			inputFile = CreateInputFile(conf, inputData);
			conf.Set("textinputformat.record.delimiter", "+-");
			for (int bufferSize_4 = 1; bufferSize_4 <= inputData.Length; bufferSize_4++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize_4);
					TestSplitRecordsForFile(conf, splitSize, inputData.Length, inputFile);
				}
			}
			// multi char delimiter with newline as start of the delimiter
			inputData = "abc\n+def\n+ghi\n+jkl\n+mno";
			inputFile = CreateInputFile(conf, inputData);
			conf.Set("textinputformat.record.delimiter", "\n+");
			for (int bufferSize_5 = 1; bufferSize_5 <= inputData.Length; bufferSize_5++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize_5);
					TestSplitRecordsForFile(conf, splitSize, inputData.Length, inputFile);
				}
			}
			// multi char delimiter with newline in delimiter and in data
			inputData = "abc\ndef+\nghi+\njkl\nmno";
			inputFile = CreateInputFile(conf, inputData);
			conf.Set("textinputformat.record.delimiter", "+\n");
			for (int bufferSize_6 = 1; bufferSize_6 <= inputData.Length; bufferSize_6++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize_6);
					TestSplitRecordsForFile(conf, splitSize, inputData.Length, inputFile);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUncompressedInputContainingCRLF()
		{
			Configuration conf = new Configuration();
			string inputData = "a\r\nb\rc\nd\r\n";
			Path inputFile = CreateInputFile(conf, inputData);
			for (int bufferSize = 1; bufferSize <= inputData.Length; bufferSize++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize);
					TestSplitRecordsForFile(conf, splitSize, inputData.Length, inputFile);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUncompressedInputCustomDelimiterPosValue()
		{
			Configuration conf = new Configuration();
			conf.SetInt("io.file.buffer.size", 10);
			conf.SetInt(LineRecordReader.MaxLineLength, int.MaxValue);
			string inputData = "abcdefghij++kl++mno";
			Path inputFile = CreateInputFile(conf, inputData);
			string delimiter = "++";
			byte[] recordDelimiterBytes = Sharpen.Runtime.GetBytesForString(delimiter, Charsets
				.Utf8);
			// the first split must contain two records to make sure that it also pulls
			// in the record from the 2nd split
			int splitLength = 15;
			FileSplit split = new FileSplit(inputFile, 0, splitLength, (string[])null);
			LineRecordReader reader = new LineRecordReader(conf, split, recordDelimiterBytes);
			LongWritable key = new LongWritable();
			Text value = new Text();
			// Get first record: "abcdefghij"
			NUnit.Framework.Assert.IsTrue("Expected record got nothing", reader.Next(key, value
				));
			NUnit.Framework.Assert.AreEqual("Wrong length for record value", 10, value.GetLength
				());
			// Position should be 12 right after "abcdefghij++"
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 12, reader.GetPos
				());
			// Get second record: "kl"
			NUnit.Framework.Assert.IsTrue("Expected record got nothing", reader.Next(key, value
				));
			NUnit.Framework.Assert.AreEqual("Wrong length for record value", 2, value.GetLength
				());
			// Position should be 16 right after "abcdefghij++kl++"
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 16, reader.GetPos
				());
			// Get third record: "mno"
			NUnit.Framework.Assert.IsTrue("Expected record got nothing", reader.Next(key, value
				));
			NUnit.Framework.Assert.AreEqual("Wrong length for record value", 3, value.GetLength
				());
			// Position should be 19 right after "abcdefghij++kl++mno"
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 19, reader.GetPos
				());
			NUnit.Framework.Assert.IsFalse(reader.Next(key, value));
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 19, reader.GetPos
				());
			reader.Close();
			// No record is in the second split because the second split will drop
			// the first record, which was already reported by the first split.
			split = new FileSplit(inputFile, splitLength, inputData.Length - splitLength, (string
				[])null);
			reader = new LineRecordReader(conf, split, recordDelimiterBytes);
			// The position should be 19 right after "abcdefghij++kl++mno" and should
			// not change
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 19, reader.GetPos
				());
			NUnit.Framework.Assert.IsFalse("Unexpected record returned", reader.Next(key, value
				));
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 19, reader.GetPos
				());
			reader.Close();
			// multi char delimiter with starting part of the delimiter in the data
			inputData = "abcd+efgh++ijk++mno";
			inputFile = CreateInputFile(conf, inputData);
			splitLength = 5;
			split = new FileSplit(inputFile, 0, splitLength, (string[])null);
			reader = new LineRecordReader(conf, split, recordDelimiterBytes);
			// Get first record: "abcd+efgh"
			NUnit.Framework.Assert.IsTrue("Expected record got nothing", reader.Next(key, value
				));
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 11, reader.GetPos
				());
			NUnit.Framework.Assert.AreEqual("Wrong length for record value", 9, value.GetLength
				());
			// should have jumped over the delimiter, no record
			NUnit.Framework.Assert.IsFalse("Unexpected record returned", reader.Next(key, value
				));
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 11, reader.GetPos
				());
			reader.Close();
			// next split: check for duplicate or dropped records
			split = new FileSplit(inputFile, splitLength, inputData.Length - splitLength, (string
				[])null);
			reader = new LineRecordReader(conf, split, recordDelimiterBytes);
			// Get second record: "ijk" first in this split
			NUnit.Framework.Assert.IsTrue("Expected record got nothing", reader.Next(key, value
				));
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 16, reader.GetPos
				());
			NUnit.Framework.Assert.AreEqual("Wrong length for record value", 3, value.GetLength
				());
			// Get third record: "mno" second in this split
			NUnit.Framework.Assert.IsTrue("Expected record got nothing", reader.Next(key, value
				));
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 19, reader.GetPos
				());
			NUnit.Framework.Assert.AreEqual("Wrong length for record value", 3, value.GetLength
				());
			// should be at the end of the input
			NUnit.Framework.Assert.IsFalse(reader.Next(key, value));
			NUnit.Framework.Assert.AreEqual("Wrong position after record read", 19, reader.GetPos
				());
			reader.Close();
			inputData = "abcd|efgh|+|ij|kl|+|mno|pqr";
			inputFile = CreateInputFile(conf, inputData);
			delimiter = "|+|";
			recordDelimiterBytes = Sharpen.Runtime.GetBytesForString(delimiter, Charsets.Utf8
				);
			// walking over the buffer and split sizes checks for proper processing
			// of the ambiguous bytes of the delimiter
			for (int bufferSize = 1; bufferSize <= inputData.Length; bufferSize++)
			{
				for (int splitSize = 1; splitSize < inputData.Length; splitSize++)
				{
					conf.SetInt("io.file.buffer.size", bufferSize);
					split = new FileSplit(inputFile, 0, bufferSize, (string[])null);
					reader = new LineRecordReader(conf, split, recordDelimiterBytes);
					// Get first record: "abcd|efgh" always possible
					NUnit.Framework.Assert.IsTrue("Expected record got nothing", reader.Next(key, value
						));
					NUnit.Framework.Assert.IsTrue("abcd|efgh".Equals(value.ToString()));
					NUnit.Framework.Assert.AreEqual("Wrong position after record read", 9, value.GetLength
						());
					// Position should be 12 right after "|+|"
					int recordPos = 12;
					NUnit.Framework.Assert.AreEqual("Wrong position after record read", recordPos, reader
						.GetPos());
					// get the next record: "ij|kl" if the split/buffer allows it
					if (reader.Next(key, value))
					{
						// check the record info: "ij|kl"
						NUnit.Framework.Assert.IsTrue("ij|kl".Equals(value.ToString()));
						// Position should be 20 right after "|+|"
						recordPos = 20;
						NUnit.Framework.Assert.AreEqual("Wrong position after record read", recordPos, reader
							.GetPos());
					}
					// get the third record: "mno|pqr" if the split/buffer allows it
					if (reader.Next(key, value))
					{
						// check the record info: "mno|pqr"
						NUnit.Framework.Assert.IsTrue("mno|pqr".Equals(value.ToString()));
						// Position should be 27 at the end of the string now
						recordPos = inputData.Length;
						NUnit.Framework.Assert.AreEqual("Wrong position after record read", recordPos, reader
							.GetPos());
					}
					// no more records can be read we should still be at the last position
					NUnit.Framework.Assert.IsFalse("Unexpected record returned", reader.Next(key, value
						));
					NUnit.Framework.Assert.AreEqual("Wrong position after record read", recordPos, reader
						.GetPos());
					reader.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUncompressedInputDefaultDelimiterPosValue()
		{
			Configuration conf = new Configuration();
			string inputData = "1234567890\r\n12\r\n345";
			Path inputFile = CreateInputFile(conf, inputData);
			conf.SetInt("io.file.buffer.size", 10);
			conf.SetInt(LineRecordReader.MaxLineLength, int.MaxValue);
			FileSplit split = new FileSplit(inputFile, 0, 15, (string[])null);
			LineRecordReader reader = new LineRecordReader(conf, split, null);
			LongWritable key = new LongWritable();
			Text value = new Text();
			reader.Next(key, value);
			// Get first record:"1234567890"
			NUnit.Framework.Assert.AreEqual(10, value.GetLength());
			// Position should be 12 right after "1234567890\r\n"
			NUnit.Framework.Assert.AreEqual(12, reader.GetPos());
			reader.Next(key, value);
			// Get second record:"12"
			NUnit.Framework.Assert.AreEqual(2, value.GetLength());
			// Position should be 16 right after "1234567890\r\n12\r\n"
			NUnit.Framework.Assert.AreEqual(16, reader.GetPos());
			NUnit.Framework.Assert.IsFalse(reader.Next(key, value));
			split = new FileSplit(inputFile, 15, 4, (string[])null);
			reader = new LineRecordReader(conf, split, null);
			// The second split dropped the first record "\n"
			// The position should be 16 right after "1234567890\r\n12\r\n"
			NUnit.Framework.Assert.AreEqual(16, reader.GetPos());
			reader.Next(key, value);
			// Get third record:"345"
			NUnit.Framework.Assert.AreEqual(3, value.GetLength());
			// Position should be 19 right after "1234567890\r\n12\r\n345"
			NUnit.Framework.Assert.AreEqual(19, reader.GetPos());
			NUnit.Framework.Assert.IsFalse(reader.Next(key, value));
			NUnit.Framework.Assert.AreEqual(19, reader.GetPos());
			inputData = "123456789\r\r\n";
			inputFile = CreateInputFile(conf, inputData);
			split = new FileSplit(inputFile, 0, 12, (string[])null);
			reader = new LineRecordReader(conf, split, null);
			reader.Next(key, value);
			// Get first record:"123456789"
			NUnit.Framework.Assert.AreEqual(9, value.GetLength());
			// Position should be 10 right after "123456789\r"
			NUnit.Framework.Assert.AreEqual(10, reader.GetPos());
			reader.Next(key, value);
			// Get second record:""
			NUnit.Framework.Assert.AreEqual(0, value.GetLength());
			// Position should be 12 right after "123456789\r\r\n"
			NUnit.Framework.Assert.AreEqual(12, reader.GetPos());
			NUnit.Framework.Assert.IsFalse(reader.Next(key, value));
			NUnit.Framework.Assert.AreEqual(12, reader.GetPos());
		}
	}
}
