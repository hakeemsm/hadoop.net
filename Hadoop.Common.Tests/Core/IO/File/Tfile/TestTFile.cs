using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>test tfile features.</summary>
	public class TestTFile : TestCase
	{
		private static string Root = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
			);

		private FileSystem fs;

		private Configuration conf;

		private const int minBlockSize = 512;

		private const int largeVal = 3 * 1024 * 1024;

		private const string localFormatter = "%010d";

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			conf = new Configuration();
			fs = FileSystem.Get(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
		}

		// do nothing
		// read a key from the scanner
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] ReadKey(TFile.Reader.Scanner scanner)
		{
			int keylen = scanner.Entry().GetKeyLength();
			byte[] read = new byte[keylen];
			scanner.Entry().GetKey(read);
			return read;
		}

		// read a value from the scanner
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] ReadValue(TFile.Reader.Scanner scanner)
		{
			int valueLen = scanner.Entry().GetValueLength();
			byte[] read = new byte[valueLen];
			scanner.Entry().GetValue(read);
			return read;
		}

		// read a long value from the scanner
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] ReadLongValue(TFile.Reader.Scanner scanner, int len)
		{
			DataInputStream din = scanner.Entry().GetValueStream();
			byte[] b = new byte[len];
			din.ReadFully(b);
			din.Close();
			return b;
		}

		// write some records into the tfile
		// write them twice
		/// <exception cref="System.IO.IOException"/>
		private int WriteSomeRecords(TFile.Writer writer, int start, int n)
		{
			string value = "value";
			for (int i = start; i < (start + n); i++)
			{
				string key = string.Format(localFormatter, i);
				writer.Append(Sharpen.Runtime.GetBytesForString(key), Sharpen.Runtime.GetBytesForString
					((value + key)));
				writer.Append(Sharpen.Runtime.GetBytesForString(key), Sharpen.Runtime.GetBytesForString
					((value + key)));
			}
			return (start + n);
		}

		// read the records and check
		/// <exception cref="System.IO.IOException"/>
		private int ReadAndCheckbytes(TFile.Reader.Scanner scanner, int start, int n)
		{
			string value = "value";
			for (int i = start; i < (start + n); i++)
			{
				byte[] key = ReadKey(scanner);
				byte[] val = ReadValue(scanner);
				string keyStr = string.Format(localFormatter, i);
				string valStr = value + keyStr;
				NUnit.Framework.Assert.IsTrue("btyes for keys do not match " + keyStr + " " + Sharpen.Runtime.GetStringForBytes
					(key), Arrays.Equals(Sharpen.Runtime.GetBytesForString(keyStr), key));
				NUnit.Framework.Assert.IsTrue("bytes for vals do not match " + valStr + " " + Sharpen.Runtime.GetStringForBytes
					(val), Arrays.Equals(Sharpen.Runtime.GetBytesForString(valStr), val));
				NUnit.Framework.Assert.IsTrue(scanner.Advance());
				key = ReadKey(scanner);
				val = ReadValue(scanner);
				NUnit.Framework.Assert.IsTrue("btyes for keys do not match", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					(keyStr), key));
				NUnit.Framework.Assert.IsTrue("bytes for vals do not match", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					(valStr), val));
				NUnit.Framework.Assert.IsTrue(scanner.Advance());
			}
			return (start + n);
		}

		// write some large records
		// write them twice
		/// <exception cref="System.IO.IOException"/>
		private int WriteLargeRecords(TFile.Writer writer, int start, int n)
		{
			byte[] value = new byte[largeVal];
			for (int i = start; i < (start + n); i++)
			{
				string key = string.Format(localFormatter, i);
				writer.Append(Sharpen.Runtime.GetBytesForString(key), value);
				writer.Append(Sharpen.Runtime.GetBytesForString(key), value);
			}
			return (start + n);
		}

		// read large records
		// read them twice since its duplicated
		/// <exception cref="System.IO.IOException"/>
		private int ReadLargeRecords(TFile.Reader.Scanner scanner, int start, int n)
		{
			for (int i = start; i < (start + n); i++)
			{
				byte[] key = ReadKey(scanner);
				string keyStr = string.Format(localFormatter, i);
				NUnit.Framework.Assert.IsTrue("btyes for keys do not match", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					(keyStr), key));
				scanner.Advance();
				key = ReadKey(scanner);
				NUnit.Framework.Assert.IsTrue("btyes for keys do not match", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					(keyStr), key));
				scanner.Advance();
			}
			return (start + n);
		}

		// write empty keys and values
		/// <exception cref="System.IO.IOException"/>
		private void WriteEmptyRecords(TFile.Writer writer, int n)
		{
			byte[] key = new byte[0];
			byte[] value = new byte[0];
			for (int i = 0; i < n; i++)
			{
				writer.Append(key, value);
			}
		}

		// read empty keys and values
		/// <exception cref="System.IO.IOException"/>
		private void ReadEmptyRecords(TFile.Reader.Scanner scanner, int n)
		{
			byte[] key = new byte[0];
			byte[] value = new byte[0];
			byte[] readKey = null;
			byte[] readValue = null;
			for (int i = 0; i < n; i++)
			{
				readKey = ReadKey(scanner);
				readValue = ReadValue(scanner);
				NUnit.Framework.Assert.IsTrue("failed to match keys", Arrays.Equals(readKey, key)
					);
				NUnit.Framework.Assert.IsTrue("failed to match values", Arrays.Equals(readValue, 
					value));
				NUnit.Framework.Assert.IsTrue("failed to advance cursor", scanner.Advance());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int WritePrepWithKnownLength(TFile.Writer writer, int start, int n)
		{
			// get the length of the key
			string key = string.Format(localFormatter, start);
			int keyLen = Sharpen.Runtime.GetBytesForString(key).Length;
			string value = "value" + key;
			int valueLen = Sharpen.Runtime.GetBytesForString(value).Length;
			for (int i = start; i < (start + n); i++)
			{
				DataOutputStream @out = writer.PrepareAppendKey(keyLen);
				string localKey = string.Format(localFormatter, i);
				@out.Write(Sharpen.Runtime.GetBytesForString(localKey));
				@out.Close();
				@out = writer.PrepareAppendValue(valueLen);
				string localValue = "value" + localKey;
				@out.Write(Sharpen.Runtime.GetBytesForString(localValue));
				@out.Close();
			}
			return (start + n);
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadPrepWithKnownLength(TFile.Reader.Scanner scanner, int start, int 
			n)
		{
			for (int i = start; i < (start + n); i++)
			{
				string key = string.Format(localFormatter, i);
				byte[] read = ReadKey(scanner);
				NUnit.Framework.Assert.IsTrue("keys not equal", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					(key), read));
				string value = "value" + key;
				read = ReadValue(scanner);
				NUnit.Framework.Assert.IsTrue("values not equal", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					(value), read));
				scanner.Advance();
			}
			return (start + n);
		}

		/// <exception cref="System.IO.IOException"/>
		private int WritePrepWithUnkownLength(TFile.Writer writer, int start, int n)
		{
			for (int i = start; i < (start + n); i++)
			{
				DataOutputStream @out = writer.PrepareAppendKey(-1);
				string localKey = string.Format(localFormatter, i);
				@out.Write(Sharpen.Runtime.GetBytesForString(localKey));
				@out.Close();
				string value = "value" + localKey;
				@out = writer.PrepareAppendValue(-1);
				@out.Write(Sharpen.Runtime.GetBytesForString(value));
				@out.Close();
			}
			return (start + n);
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadPrepWithUnknownLength(TFile.Reader.Scanner scanner, int start, int
			 n)
		{
			for (int i = start; i < start; i++)
			{
				string key = string.Format(localFormatter, i);
				byte[] read = ReadKey(scanner);
				NUnit.Framework.Assert.IsTrue("keys not equal", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					(key), read));
				try
				{
					read = ReadValue(scanner);
					NUnit.Framework.Assert.IsTrue(false);
				}
				catch (IOException)
				{
				}
				// should have thrown exception
				string value = "value" + key;
				read = ReadLongValue(scanner, Sharpen.Runtime.GetBytesForString(value).Length);
				NUnit.Framework.Assert.IsTrue("values nto equal", Arrays.Equals(read, Sharpen.Runtime.GetBytesForString
					(value)));
				scanner.Advance();
			}
			return (start + n);
		}

		private byte[] GetSomeKey(int rowId)
		{
			return Sharpen.Runtime.GetBytesForString(string.Format(localFormatter, rowId));
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteRecords(TFile.Writer writer)
		{
			WriteEmptyRecords(writer, 10);
			int ret = WriteSomeRecords(writer, 0, 100);
			ret = WriteLargeRecords(writer, ret, 1);
			ret = WritePrepWithKnownLength(writer, ret, 40);
			ret = WritePrepWithUnkownLength(writer, ret, 50);
			writer.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadAllRecords(TFile.Reader.Scanner scanner)
		{
			ReadEmptyRecords(scanner, 10);
			int ret = ReadAndCheckbytes(scanner, 0, 100);
			ret = ReadLargeRecords(scanner, ret, 1);
			ret = ReadPrepWithKnownLength(scanner, ret, 40);
			ret = ReadPrepWithUnknownLength(scanner, ret, 50);
		}

		/// <exception cref="System.IO.IOException"/>
		private FSDataOutputStream CreateFSOutput(Path name)
		{
			if (fs.Exists(name))
			{
				fs.Delete(name, true);
			}
			FSDataOutputStream fout = fs.Create(name);
			return fout;
		}

		/// <summary>test none codecs</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void BasicWithSomeCodec(string codec)
		{
			Path ncTFile = new Path(Root, "basic.tfile");
			FSDataOutputStream fout = CreateFSOutput(ncTFile);
			TFile.Writer writer = new TFile.Writer(fout, minBlockSize, codec, "memcmp", conf);
			WriteRecords(writer);
			fout.Close();
			FSDataInputStream fin = fs.Open(ncTFile);
			TFile.Reader reader = new TFile.Reader(fs.Open(ncTFile), fs.GetFileStatus(ncTFile
				).GetLen(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			ReadAllRecords(scanner);
			scanner.SeekTo(GetSomeKey(50));
			NUnit.Framework.Assert.IsTrue("location lookup failed", scanner.SeekTo(GetSomeKey
				(50)));
			// read the key and see if it matches
			byte[] readKey = ReadKey(scanner);
			NUnit.Framework.Assert.IsTrue("seeked key does not match", Arrays.Equals(GetSomeKey
				(50), readKey));
			scanner.SeekTo(new byte[0]);
			byte[] val1 = ReadValue(scanner);
			scanner.SeekTo(new byte[0]);
			byte[] val2 = ReadValue(scanner);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(val1, val2));
			// check for lowerBound
			scanner.LowerBound(GetSomeKey(50));
			NUnit.Framework.Assert.IsTrue("locaton lookup failed", scanner.currentLocation.CompareTo
				(reader.End()) < 0);
			readKey = ReadKey(scanner);
			NUnit.Framework.Assert.IsTrue("seeked key does not match", Arrays.Equals(readKey, 
				GetSomeKey(50)));
			// check for upper bound
			scanner.UpperBound(GetSomeKey(50));
			NUnit.Framework.Assert.IsTrue("location lookup failed", scanner.currentLocation.CompareTo
				(reader.End()) < 0);
			readKey = ReadKey(scanner);
			NUnit.Framework.Assert.IsTrue("seeked key does not match", Arrays.Equals(readKey, 
				GetSomeKey(51)));
			scanner.Close();
			// test for a range of scanner
			scanner = reader.CreateScannerByKey(GetSomeKey(10), GetSomeKey(60));
			ReadAndCheckbytes(scanner, 10, 50);
			NUnit.Framework.Assert.IsFalse(scanner.Advance());
			scanner.Close();
			reader.Close();
			fin.Close();
			fs.Delete(ncTFile, true);
		}

		// unsorted with some codec
		/// <exception cref="System.IO.IOException"/>
		internal virtual void UnsortedWithSomeCodec(string codec)
		{
			Path uTfile = new Path(Root, "unsorted.tfile");
			FSDataOutputStream fout = CreateFSOutput(uTfile);
			TFile.Writer writer = new TFile.Writer(fout, minBlockSize, codec, null, conf);
			WriteRecords(writer);
			writer.Close();
			fout.Close();
			FSDataInputStream fin = fs.Open(uTfile);
			TFile.Reader reader = new TFile.Reader(fs.Open(uTfile), fs.GetFileStatus(uTfile).
				GetLen(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			ReadAllRecords(scanner);
			scanner.Close();
			reader.Close();
			fin.Close();
			fs.Delete(uTfile, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTFileFeatures()
		{
			BasicWithSomeCodec("none");
			BasicWithSomeCodec("gz");
		}

		// test unsorted t files.
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnsortedTFileFeatures()
		{
			UnsortedWithSomeCodec("none");
			UnsortedWithSomeCodec("gz");
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteNumMetablocks(TFile.Writer writer, string compression, int n)
		{
			for (int i = 0; i < n; i++)
			{
				DataOutputStream dout = writer.PrepareMetaBlock("TfileMeta" + i, compression);
				byte[] b = Sharpen.Runtime.GetBytesForString(("something to test" + i));
				dout.Write(b);
				dout.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void SomeTestingWithMetaBlock(TFile.Writer writer, string compression)
		{
			DataOutputStream dout = null;
			WriteNumMetablocks(writer, compression, 10);
			try
			{
				dout = writer.PrepareMetaBlock("TfileMeta1", compression);
				NUnit.Framework.Assert.IsTrue(false);
			}
			catch (MetaBlockAlreadyExists)
			{
			}
			// avoid this exception
			dout = writer.PrepareMetaBlock("TFileMeta100", compression);
			dout.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadNumMetablocks(TFile.Reader reader, int n)
		{
			int len = Sharpen.Runtime.GetBytesForString(("something to test" + 0)).Length;
			for (int i = 0; i < n; i++)
			{
				DataInputStream din = reader.GetMetaBlock("TfileMeta" + i);
				byte[] b = new byte[len];
				din.ReadFully(b);
				NUnit.Framework.Assert.IsTrue("faield to match metadata", Arrays.Equals(Sharpen.Runtime.GetBytesForString
					(("something to test" + i)), b));
				din.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void SomeReadingWithMetaBlock(TFile.Reader reader)
		{
			DataInputStream din = null;
			ReadNumMetablocks(reader, 10);
			try
			{
				din = reader.GetMetaBlock("NO ONE");
				NUnit.Framework.Assert.IsTrue(false);
			}
			catch (MetaBlockDoesNotExist)
			{
			}
			// should catch
			din = reader.GetMetaBlock("TFileMeta100");
			int read = din.Read();
			NUnit.Framework.Assert.IsTrue("check for status", (read == -1));
			din.Close();
		}

		// test meta blocks for tfiles
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMetaBlocks()
		{
			Path mFile = new Path(Root, "meta.tfile");
			FSDataOutputStream fout = CreateFSOutput(mFile);
			TFile.Writer writer = new TFile.Writer(fout, minBlockSize, "none", null, conf);
			SomeTestingWithMetaBlock(writer, "none");
			writer.Close();
			fout.Close();
			FSDataInputStream fin = fs.Open(mFile);
			TFile.Reader reader = new TFile.Reader(fin, fs.GetFileStatus(mFile).GetLen(), conf
				);
			SomeReadingWithMetaBlock(reader);
			fs.Delete(mFile, true);
			reader.Close();
			fin.Close();
		}
	}
}
