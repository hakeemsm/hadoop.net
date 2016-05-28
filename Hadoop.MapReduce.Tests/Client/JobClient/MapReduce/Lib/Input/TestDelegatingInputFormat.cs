using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestDelegatingInputFormat : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestSplitting()
		{
			Job job = Job.GetInstance();
			MiniDFSCluster dfs = null;
			try
			{
				dfs = new MiniDFSCluster.Builder(job.GetConfiguration()).NumDataNodes(4).Racks(new 
					string[] { "/rack0", "/rack0", "/rack1", "/rack1" }).Hosts(new string[] { "host0"
					, "host1", "host2", "host3" }).Build();
				FileSystem fs = dfs.GetFileSystem();
				Path path = GetPath("/foo/bar", fs);
				Path path2 = GetPath("/foo/baz", fs);
				Path path3 = GetPath("/bar/bar", fs);
				Path path4 = GetPath("/bar/baz", fs);
				int numSplits = 100;
				FileInputFormat.SetMaxInputSplitSize(job, fs.GetFileStatus(path).GetLen() / numSplits
					);
				MultipleInputs.AddInputPath(job, path, typeof(TextInputFormat), typeof(TestDelegatingInputFormat.MapClass
					));
				MultipleInputs.AddInputPath(job, path2, typeof(TextInputFormat), typeof(TestDelegatingInputFormat.MapClass2
					));
				MultipleInputs.AddInputPath(job, path3, typeof(KeyValueTextInputFormat), typeof(TestDelegatingInputFormat.MapClass
					));
				MultipleInputs.AddInputPath(job, path4, typeof(TextInputFormat), typeof(TestDelegatingInputFormat.MapClass2
					));
				DelegatingInputFormat inFormat = new DelegatingInputFormat();
				int[] bins = new int[3];
				foreach (InputSplit split in (IList<InputSplit>)inFormat.GetSplits(job))
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
				NUnit.Framework.Assert.AreEqual("count is not equal to num splits", numSplits, bins
					[0]);
				NUnit.Framework.Assert.AreEqual("count is not equal to num splits", numSplits, bins
					[1]);
				NUnit.Framework.Assert.AreEqual("count is not equal to 2 * num splits", numSplits
					 * 2, bins[2]);
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
		}

		internal class MapClass2 : TestDelegatingInputFormat.MapClass
		{
		}
	}
}
