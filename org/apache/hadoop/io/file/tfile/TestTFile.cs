using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>test tfile features.</summary>
	public class TestTFile : NUnit.Framework.TestCase
	{
		private static string ROOT = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
			);

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.conf.Configuration conf;

		private const int minBlockSize = 512;

		private const int largeVal = 3 * 1024 * 1024;

		private const string localFormatter = "%010d";

		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			fs = org.apache.hadoop.fs.FileSystem.get(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
		}

		// do nothing
		// read a key from the scanner
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] readKey(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner)
		{
			int keylen = scanner.entry().getKeyLength();
			byte[] read = new byte[keylen];
			scanner.entry().getKey(read);
			return read;
		}

		// read a value from the scanner
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] readValue(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner)
		{
			int valueLen = scanner.entry().getValueLength();
			byte[] read = new byte[valueLen];
			scanner.entry().getValue(read);
			return read;
		}

		// read a long value from the scanner
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] readLongValue(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner, int len)
		{
			java.io.DataInputStream din = scanner.entry().getValueStream();
			byte[] b = new byte[len];
			din.readFully(b);
			din.close();
			return b;
		}

		// write some records into the tfile
		// write them twice
		/// <exception cref="System.IO.IOException"/>
		private int writeSomeRecords(org.apache.hadoop.io.file.tfile.TFile.Writer writer, 
			int start, int n)
		{
			string value = "value";
			for (int i = start; i < (start + n); i++)
			{
				string key = string.format(localFormatter, i);
				writer.append(Sharpen.Runtime.getBytesForString(key), Sharpen.Runtime.getBytesForString
					((value + key)));
				writer.append(Sharpen.Runtime.getBytesForString(key), Sharpen.Runtime.getBytesForString
					((value + key)));
			}
			return (start + n);
		}

		// read the records and check
		/// <exception cref="System.IO.IOException"/>
		private int readAndCheckbytes(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner, int start, int n)
		{
			string value = "value";
			for (int i = start; i < (start + n); i++)
			{
				byte[] key = readKey(scanner);
				byte[] val = readValue(scanner);
				string keyStr = string.format(localFormatter, i);
				string valStr = value + keyStr;
				NUnit.Framework.Assert.IsTrue("btyes for keys do not match " + keyStr + " " + Sharpen.Runtime.getStringForBytes
					(key), java.util.Arrays.equals(Sharpen.Runtime.getBytesForString(keyStr), key));
				NUnit.Framework.Assert.IsTrue("bytes for vals do not match " + valStr + " " + Sharpen.Runtime.getStringForBytes
					(val), java.util.Arrays.equals(Sharpen.Runtime.getBytesForString(valStr), val));
				NUnit.Framework.Assert.IsTrue(scanner.advance());
				key = readKey(scanner);
				val = readValue(scanner);
				NUnit.Framework.Assert.IsTrue("btyes for keys do not match", java.util.Arrays.equals
					(Sharpen.Runtime.getBytesForString(keyStr), key));
				NUnit.Framework.Assert.IsTrue("bytes for vals do not match", java.util.Arrays.equals
					(Sharpen.Runtime.getBytesForString(valStr), val));
				NUnit.Framework.Assert.IsTrue(scanner.advance());
			}
			return (start + n);
		}

		// write some large records
		// write them twice
		/// <exception cref="System.IO.IOException"/>
		private int writeLargeRecords(org.apache.hadoop.io.file.tfile.TFile.Writer writer
			, int start, int n)
		{
			byte[] value = new byte[largeVal];
			for (int i = start; i < (start + n); i++)
			{
				string key = string.format(localFormatter, i);
				writer.append(Sharpen.Runtime.getBytesForString(key), value);
				writer.append(Sharpen.Runtime.getBytesForString(key), value);
			}
			return (start + n);
		}

		// read large records
		// read them twice since its duplicated
		/// <exception cref="System.IO.IOException"/>
		private int readLargeRecords(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner, int start, int n)
		{
			for (int i = start; i < (start + n); i++)
			{
				byte[] key = readKey(scanner);
				string keyStr = string.format(localFormatter, i);
				NUnit.Framework.Assert.IsTrue("btyes for keys do not match", java.util.Arrays.equals
					(Sharpen.Runtime.getBytesForString(keyStr), key));
				scanner.advance();
				key = readKey(scanner);
				NUnit.Framework.Assert.IsTrue("btyes for keys do not match", java.util.Arrays.equals
					(Sharpen.Runtime.getBytesForString(keyStr), key));
				scanner.advance();
			}
			return (start + n);
		}

		// write empty keys and values
		/// <exception cref="System.IO.IOException"/>
		private void writeEmptyRecords(org.apache.hadoop.io.file.tfile.TFile.Writer writer
			, int n)
		{
			byte[] key = new byte[0];
			byte[] value = new byte[0];
			for (int i = 0; i < n; i++)
			{
				writer.append(key, value);
			}
		}

		// read empty keys and values
		/// <exception cref="System.IO.IOException"/>
		private void readEmptyRecords(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner, int n)
		{
			byte[] key = new byte[0];
			byte[] value = new byte[0];
			byte[] readKey = null;
			byte[] readValue = null;
			for (int i = 0; i < n; i++)
			{
				readKey = readKey(scanner);
				readValue = readValue(scanner);
				NUnit.Framework.Assert.IsTrue("failed to match keys", java.util.Arrays.equals(readKey
					, key));
				NUnit.Framework.Assert.IsTrue("failed to match values", java.util.Arrays.equals(readValue
					, value));
				NUnit.Framework.Assert.IsTrue("failed to advance cursor", scanner.advance());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int writePrepWithKnownLength(org.apache.hadoop.io.file.tfile.TFile.Writer
			 writer, int start, int n)
		{
			// get the length of the key
			string key = string.format(localFormatter, start);
			int keyLen = Sharpen.Runtime.getBytesForString(key).Length;
			string value = "value" + key;
			int valueLen = Sharpen.Runtime.getBytesForString(value).Length;
			for (int i = start; i < (start + n); i++)
			{
				java.io.DataOutputStream @out = writer.prepareAppendKey(keyLen);
				string localKey = string.format(localFormatter, i);
				@out.write(Sharpen.Runtime.getBytesForString(localKey));
				@out.close();
				@out = writer.prepareAppendValue(valueLen);
				string localValue = "value" + localKey;
				@out.write(Sharpen.Runtime.getBytesForString(localValue));
				@out.close();
			}
			return (start + n);
		}

		/// <exception cref="System.IO.IOException"/>
		private int readPrepWithKnownLength(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner, int start, int n)
		{
			for (int i = start; i < (start + n); i++)
			{
				string key = string.format(localFormatter, i);
				byte[] read = readKey(scanner);
				NUnit.Framework.Assert.IsTrue("keys not equal", java.util.Arrays.equals(Sharpen.Runtime.getBytesForString
					(key), read));
				string value = "value" + key;
				read = readValue(scanner);
				NUnit.Framework.Assert.IsTrue("values not equal", java.util.Arrays.equals(Sharpen.Runtime.getBytesForString
					(value), read));
				scanner.advance();
			}
			return (start + n);
		}

		/// <exception cref="System.IO.IOException"/>
		private int writePrepWithUnkownLength(org.apache.hadoop.io.file.tfile.TFile.Writer
			 writer, int start, int n)
		{
			for (int i = start; i < (start + n); i++)
			{
				java.io.DataOutputStream @out = writer.prepareAppendKey(-1);
				string localKey = string.format(localFormatter, i);
				@out.write(Sharpen.Runtime.getBytesForString(localKey));
				@out.close();
				string value = "value" + localKey;
				@out = writer.prepareAppendValue(-1);
				@out.write(Sharpen.Runtime.getBytesForString(value));
				@out.close();
			}
			return (start + n);
		}

		/// <exception cref="System.IO.IOException"/>
		private int readPrepWithUnknownLength(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner, int start, int n)
		{
			for (int i = start; i < start; i++)
			{
				string key = string.format(localFormatter, i);
				byte[] read = readKey(scanner);
				NUnit.Framework.Assert.IsTrue("keys not equal", java.util.Arrays.equals(Sharpen.Runtime.getBytesForString
					(key), read));
				try
				{
					read = readValue(scanner);
					NUnit.Framework.Assert.IsTrue(false);
				}
				catch (System.IO.IOException)
				{
				}
				// should have thrown exception
				string value = "value" + key;
				read = readLongValue(scanner, Sharpen.Runtime.getBytesForString(value).Length);
				NUnit.Framework.Assert.IsTrue("values nto equal", java.util.Arrays.equals(read, Sharpen.Runtime.getBytesForString
					(value)));
				scanner.advance();
			}
			return (start + n);
		}

		private byte[] getSomeKey(int rowId)
		{
			return Sharpen.Runtime.getBytesForString(string.format(localFormatter, rowId));
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeRecords(org.apache.hadoop.io.file.tfile.TFile.Writer writer)
		{
			writeEmptyRecords(writer, 10);
			int ret = writeSomeRecords(writer, 0, 100);
			ret = writeLargeRecords(writer, ret, 1);
			ret = writePrepWithKnownLength(writer, ret, 40);
			ret = writePrepWithUnkownLength(writer, ret, 50);
			writer.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void readAllRecords(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner 
			scanner)
		{
			readEmptyRecords(scanner, 10);
			int ret = readAndCheckbytes(scanner, 0, 100);
			ret = readLargeRecords(scanner, ret, 1);
			ret = readPrepWithKnownLength(scanner, ret, 40);
			ret = readPrepWithUnknownLength(scanner, ret, 50);
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FSDataOutputStream createFSOutput(org.apache.hadoop.fs.Path
			 name)
		{
			if (fs.exists(name))
			{
				fs.delete(name, true);
			}
			org.apache.hadoop.fs.FSDataOutputStream fout = fs.create(name);
			return fout;
		}

		/// <summary>test none codecs</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void basicWithSomeCodec(string codec)
		{
			org.apache.hadoop.fs.Path ncTFile = new org.apache.hadoop.fs.Path(ROOT, "basic.tfile"
				);
			org.apache.hadoop.fs.FSDataOutputStream fout = createFSOutput(ncTFile);
			org.apache.hadoop.io.file.tfile.TFile.Writer writer = new org.apache.hadoop.io.file.tfile.TFile.Writer
				(fout, minBlockSize, codec, "memcmp", conf);
			writeRecords(writer);
			fout.close();
			org.apache.hadoop.fs.FSDataInputStream fin = fs.open(ncTFile);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(ncTFile), fs.getFileStatus(ncTFile).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			readAllRecords(scanner);
			scanner.seekTo(getSomeKey(50));
			NUnit.Framework.Assert.IsTrue("location lookup failed", scanner.seekTo(getSomeKey
				(50)));
			// read the key and see if it matches
			byte[] readKey = readKey(scanner);
			NUnit.Framework.Assert.IsTrue("seeked key does not match", java.util.Arrays.equals
				(getSomeKey(50), readKey));
			scanner.seekTo(new byte[0]);
			byte[] val1 = readValue(scanner);
			scanner.seekTo(new byte[0]);
			byte[] val2 = readValue(scanner);
			NUnit.Framework.Assert.IsTrue(java.util.Arrays.equals(val1, val2));
			// check for lowerBound
			scanner.lowerBound(getSomeKey(50));
			NUnit.Framework.Assert.IsTrue("locaton lookup failed", scanner.currentLocation.compareTo
				(reader.end()) < 0);
			readKey = readKey(scanner);
			NUnit.Framework.Assert.IsTrue("seeked key does not match", java.util.Arrays.equals
				(readKey, getSomeKey(50)));
			// check for upper bound
			scanner.upperBound(getSomeKey(50));
			NUnit.Framework.Assert.IsTrue("location lookup failed", scanner.currentLocation.compareTo
				(reader.end()) < 0);
			readKey = readKey(scanner);
			NUnit.Framework.Assert.IsTrue("seeked key does not match", java.util.Arrays.equals
				(readKey, getSomeKey(51)));
			scanner.close();
			// test for a range of scanner
			scanner = reader.createScannerByKey(getSomeKey(10), getSomeKey(60));
			readAndCheckbytes(scanner, 10, 50);
			NUnit.Framework.Assert.IsFalse(scanner.advance());
			scanner.close();
			reader.close();
			fin.close();
			fs.delete(ncTFile, true);
		}

		// unsorted with some codec
		/// <exception cref="System.IO.IOException"/>
		internal virtual void unsortedWithSomeCodec(string codec)
		{
			org.apache.hadoop.fs.Path uTfile = new org.apache.hadoop.fs.Path(ROOT, "unsorted.tfile"
				);
			org.apache.hadoop.fs.FSDataOutputStream fout = createFSOutput(uTfile);
			org.apache.hadoop.io.file.tfile.TFile.Writer writer = new org.apache.hadoop.io.file.tfile.TFile.Writer
				(fout, minBlockSize, codec, null, conf);
			writeRecords(writer);
			writer.close();
			fout.close();
			org.apache.hadoop.fs.FSDataInputStream fin = fs.open(uTfile);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(uTfile), fs.getFileStatus(uTfile).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			readAllRecords(scanner);
			scanner.close();
			reader.close();
			fin.close();
			fs.delete(uTfile, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testTFileFeatures()
		{
			basicWithSomeCodec("none");
			basicWithSomeCodec("gz");
		}

		// test unsorted t files.
		/// <exception cref="System.IO.IOException"/>
		public virtual void testUnsortedTFileFeatures()
		{
			unsortedWithSomeCodec("none");
			unsortedWithSomeCodec("gz");
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeNumMetablocks(org.apache.hadoop.io.file.tfile.TFile.Writer writer
			, string compression, int n)
		{
			for (int i = 0; i < n; i++)
			{
				java.io.DataOutputStream dout = writer.prepareMetaBlock("TfileMeta" + i, compression
					);
				byte[] b = Sharpen.Runtime.getBytesForString(("something to test" + i));
				dout.write(b);
				dout.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void someTestingWithMetaBlock(org.apache.hadoop.io.file.tfile.TFile.Writer
			 writer, string compression)
		{
			java.io.DataOutputStream dout = null;
			writeNumMetablocks(writer, compression, 10);
			try
			{
				dout = writer.prepareMetaBlock("TfileMeta1", compression);
				NUnit.Framework.Assert.IsTrue(false);
			}
			catch (org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists)
			{
			}
			// avoid this exception
			dout = writer.prepareMetaBlock("TFileMeta100", compression);
			dout.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void readNumMetablocks(org.apache.hadoop.io.file.tfile.TFile.Reader reader
			, int n)
		{
			int len = Sharpen.Runtime.getBytesForString(("something to test" + 0)).Length;
			for (int i = 0; i < n; i++)
			{
				java.io.DataInputStream din = reader.getMetaBlock("TfileMeta" + i);
				byte[] b = new byte[len];
				din.readFully(b);
				NUnit.Framework.Assert.IsTrue("faield to match metadata", java.util.Arrays.equals
					(Sharpen.Runtime.getBytesForString(("something to test" + i)), b));
				din.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void someReadingWithMetaBlock(org.apache.hadoop.io.file.tfile.TFile.Reader
			 reader)
		{
			java.io.DataInputStream din = null;
			readNumMetablocks(reader, 10);
			try
			{
				din = reader.getMetaBlock("NO ONE");
				NUnit.Framework.Assert.IsTrue(false);
			}
			catch (org.apache.hadoop.io.file.tfile.MetaBlockDoesNotExist)
			{
			}
			// should catch
			din = reader.getMetaBlock("TFileMeta100");
			int read = din.read();
			NUnit.Framework.Assert.IsTrue("check for status", (read == -1));
			din.close();
		}

		// test meta blocks for tfiles
		/// <exception cref="System.IO.IOException"/>
		public virtual void testMetaBlocks()
		{
			org.apache.hadoop.fs.Path mFile = new org.apache.hadoop.fs.Path(ROOT, "meta.tfile"
				);
			org.apache.hadoop.fs.FSDataOutputStream fout = createFSOutput(mFile);
			org.apache.hadoop.io.file.tfile.TFile.Writer writer = new org.apache.hadoop.io.file.tfile.TFile.Writer
				(fout, minBlockSize, "none", null, conf);
			someTestingWithMetaBlock(writer, "none");
			writer.close();
			fout.close();
			org.apache.hadoop.fs.FSDataInputStream fin = fs.open(mFile);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fin, fs.getFileStatus(mFile).getLen(), conf);
			someReadingWithMetaBlock(reader);
			fs.delete(mFile, true);
			reader.close();
			fin.close();
		}
	}
}
