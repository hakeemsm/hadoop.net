using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>This test exercises the ValueIterator.</summary>
	public class TestReduceTask : TestCase
	{
		internal class NullProgress : Progressable
		{
			public virtual void Progress()
			{
			}
		}

		private class Pair
		{
			internal string key;

			internal string value;

			internal Pair(string k, string v)
			{
				key = k;
				value = v;
			}
		}

		private static TestReduceTask.Pair[][] testCases = new TestReduceTask.Pair[][] { 
			new TestReduceTask.Pair[] { new TestReduceTask.Pair("k1", "v1"), new TestReduceTask.Pair
			("k2", "v2"), new TestReduceTask.Pair("k3", "v3"), new TestReduceTask.Pair("k3", 
			"v4"), new TestReduceTask.Pair("k4", "v5"), new TestReduceTask.Pair("k5", "v6") }
			, new TestReduceTask.Pair[] { new TestReduceTask.Pair(string.Empty, "v1"), new TestReduceTask.Pair
			("k1", "v2"), new TestReduceTask.Pair("k2", "v3"), new TestReduceTask.Pair("k2", 
			"v4") }, new TestReduceTask.Pair[] {  }, new TestReduceTask.Pair[] { new TestReduceTask.Pair
			("k1", "v1"), new TestReduceTask.Pair("k1", "v2"), new TestReduceTask.Pair("k1", 
			"v3"), new TestReduceTask.Pair("k1", "v4") } };

		/// <exception cref="System.IO.IOException"/>
		public virtual void RunValueIterator(Path tmpDir, TestReduceTask.Pair[] vals, Configuration
			 conf, CompressionCodec codec)
		{
			FileSystem localFs = FileSystem.GetLocal(conf);
			FileSystem rfs = ((LocalFileSystem)localFs).GetRaw();
			Path path = new Path(tmpDir, "data.in");
			IFile.Writer<Text, Text> writer = new IFile.Writer<Text, Text>(conf, rfs.Create(path
				), typeof(Text), typeof(Text), codec, null);
			foreach (TestReduceTask.Pair p in vals)
			{
				writer.Append(new Text(p.key), new Text(p.value));
			}
			writer.Close();
			RawKeyValueIterator rawItr = Merger.Merge<Text, Text>(conf, rfs, codec, new Path[
				] { path }, false, conf.GetInt(JobContext.IoSortFactor, 100), tmpDir, new Text.Comparator
				(), new TestReduceTask.NullProgress(), null, null, null);
			Task.ValuesIterator valItr = new Task.ValuesIterator<Text, Text>(rawItr, WritableComparator
				.Get(typeof(Text)), typeof(Text), typeof(Text), conf, new TestReduceTask.NullProgress
				());
			// WritableComparators are not generic
			int i = 0;
			while (valItr.More())
			{
				object key = valItr.GetKey();
				string keyString = key.ToString();
				// make sure it matches!
				NUnit.Framework.Assert.AreEqual(vals[i].key, keyString);
				// must have at least 1 value!
				NUnit.Framework.Assert.IsTrue(valItr.HasNext());
				while (valItr.HasNext())
				{
					string valueString = valItr.Next().ToString();
					// make sure the values match
					NUnit.Framework.Assert.AreEqual(vals[i].value, valueString);
					// make sure the keys match
					NUnit.Framework.Assert.AreEqual(vals[i].key, valItr.GetKey().ToString());
					i += 1;
				}
				// make sure the key hasn't changed under the hood
				NUnit.Framework.Assert.AreEqual(keyString, valItr.GetKey().ToString());
				valItr.NextKey();
			}
			NUnit.Framework.Assert.AreEqual(vals.Length, i);
			// make sure we have progress equal to 1.0
			NUnit.Framework.Assert.AreEqual(1.0f, rawItr.GetProgress().Get());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestValueIterator()
		{
			Path tmpDir = new Path("build/test/test.reduce.task");
			Configuration conf = new Configuration();
			foreach (TestReduceTask.Pair[] testCase in testCases)
			{
				RunValueIterator(tmpDir, testCase, conf, null);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestValueIteratorWithCompression()
		{
			Path tmpDir = new Path("build/test/test.reduce.task.compression");
			Configuration conf = new Configuration();
			DefaultCodec codec = new DefaultCodec();
			codec.SetConf(conf);
			foreach (TestReduceTask.Pair[] testCase in testCases)
			{
				RunValueIterator(tmpDir, testCase, conf, codec);
			}
		}
	}
}
