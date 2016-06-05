using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Split
{
	public class TestJobSplitWriter
	{
		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), "TestJobSplitWriter");

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxBlockLocationsNewSplits()
		{
			TestDir.Mkdirs();
			try
			{
				Configuration conf = new Configuration();
				conf.SetInt(MRConfig.MaxBlockLocationsKey, 4);
				Path submitDir = new Path(TestDir.GetAbsolutePath());
				FileSystem fs = FileSystem.GetLocal(conf);
				FileSplit split = new FileSplit(new Path("/some/path"), 0, 1, new string[] { "loc1"
					, "loc2", "loc3", "loc4", "loc5" });
				JobSplitWriter.CreateSplitFiles(submitDir, conf, fs, new FileSplit[] { split });
				JobSplit.TaskSplitMetaInfo[] infos = SplitMetaInfoReader.ReadSplitMetaInfo(new JobID
					(), fs, conf, submitDir);
				NUnit.Framework.Assert.AreEqual("unexpected number of splits", 1, infos.Length);
				NUnit.Framework.Assert.AreEqual("unexpected number of split locations", 4, infos[
					0].GetLocations().Length);
			}
			finally
			{
				FileUtil.FullyDelete(TestDir);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxBlockLocationsOldSplits()
		{
			TestDir.Mkdirs();
			try
			{
				Configuration conf = new Configuration();
				conf.SetInt(MRConfig.MaxBlockLocationsKey, 4);
				Path submitDir = new Path(TestDir.GetAbsolutePath());
				FileSystem fs = FileSystem.GetLocal(conf);
				FileSplit split = new FileSplit(new Path("/some/path"), 0, 1, new string[] { "loc1"
					, "loc2", "loc3", "loc4", "loc5" });
				JobSplitWriter.CreateSplitFiles(submitDir, conf, fs, new InputSplit[] { split });
				JobSplit.TaskSplitMetaInfo[] infos = SplitMetaInfoReader.ReadSplitMetaInfo(new JobID
					(), fs, conf, submitDir);
				NUnit.Framework.Assert.AreEqual("unexpected number of splits", 1, infos.Length);
				NUnit.Framework.Assert.AreEqual("unexpected number of split locations", 4, infos[
					0].GetLocations().Length);
			}
			finally
			{
				FileUtil.FullyDelete(TestDir);
			}
		}
	}
}
