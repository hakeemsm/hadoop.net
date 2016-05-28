using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestDelegatingInputFormat : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestSplitting()
		{
			JobConf conf = new JobConf();
			MiniDFSCluster dfs = null;
			try
			{
				dfs = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Racks(new string[] { "/rack0"
					, "/rack0", "/rack1", "/rack1" }).Hosts(new string[] { "host0", "host1", "host2"
					, "host3" }).Build();
				FileSystem fs = dfs.GetFileSystem();
				Path path = GetPath("/foo/bar", fs);
				Path path2 = GetPath("/foo/baz", fs);
				Path path3 = GetPath("/bar/bar", fs);
				Path path4 = GetPath("/bar/baz", fs);
				int numSplits = 100;
				MultipleInputs.AddInputPath(conf, path, typeof(TextInputFormat), typeof(TestDelegatingInputFormat.MapClass
					));
				MultipleInputs.AddInputPath(conf, path2, typeof(TextInputFormat), typeof(TestDelegatingInputFormat.MapClass2
					));
				MultipleInputs.AddInputPath(conf, path3, typeof(KeyValueTextInputFormat), typeof(
					TestDelegatingInputFormat.MapClass));
				MultipleInputs.AddInputPath(conf, path4, typeof(TextInputFormat), typeof(TestDelegatingInputFormat.MapClass2
					));
				DelegatingInputFormat inFormat = new DelegatingInputFormat();
				InputSplit[] splits = inFormat.GetSplits(conf, numSplits);
				int[] bins = new int[3];
				foreach (InputSplit split in splits)
				{
					NUnit.Framework.Assert.IsTrue(split is TaggedInputSplit);
					TaggedInputSplit tis = (TaggedInputSplit)split;
					int index = -1;
					if (tis.GetInputFormatClass().Equals(typeof(KeyValueTextInputFormat)))
					{
						// path3
						index = 0;
					}
					else
					{
						if (tis.GetMapperClass().Equals(typeof(TestDelegatingInputFormat.MapClass)))
						{
							// path
							index = 1;
						}
						else
						{
							// path2 and path4
							index = 2;
						}
					}
					bins[index]++;
				}
				// Each bin is a unique combination of a Mapper and InputFormat, and
				// DelegatingInputFormat should split each bin into numSplits splits,
				// regardless of the number of paths that use that Mapper/InputFormat
				foreach (int count in bins)
				{
					NUnit.Framework.Assert.AreEqual(numSplits, count);
				}
				NUnit.Framework.Assert.IsTrue(true);
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static Path GetPath(string location, FileSystem fs)
		{
			Path path = new Path(location);
			// create a multi-block file on hdfs
			DataOutputStream @out = fs.Create(path, true, 4096, (short)2, 512, null);
			for (int i = 0; i < 1000; ++i)
			{
				@out.WriteChars("Hello\n");
			}
			@out.Close();
			return path;
		}

		internal class MapClass : Mapper<string, string, string, string>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(string key, string value, OutputCollector<string, string>
				 output, Reporter reporter)
			{
			}

			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}

		internal class MapClass2 : TestDelegatingInputFormat.MapClass
		{
		}
	}
}
