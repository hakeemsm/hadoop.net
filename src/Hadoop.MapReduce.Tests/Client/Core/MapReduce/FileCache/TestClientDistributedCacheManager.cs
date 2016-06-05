using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Filecache
{
	public class TestClientDistributedCacheManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestClientDistributedCacheManager
			));

		private static readonly string TestRootDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToURI().ToString().Replace(' ', '+');

		private FileSystem fs;

		private Path firstCacheFile;

		private Path secondCacheFile;

		private Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration();
			fs = FileSystem.Get(conf);
			firstCacheFile = new Path(TestRootDir, "firstcachefile");
			secondCacheFile = new Path(TestRootDir, "secondcachefile");
			CreateTempFile(firstCacheFile, conf);
			CreateTempFile(secondCacheFile, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (!fs.Delete(firstCacheFile, false))
			{
				Log.Warn("Failed to delete firstcachefile");
			}
			if (!fs.Delete(secondCacheFile, false))
			{
				Log.Warn("Failed to delete secondcachefile");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDetermineTimestamps()
		{
			Job job = Job.GetInstance(conf);
			job.AddCacheFile(firstCacheFile.ToUri());
			job.AddCacheFile(secondCacheFile.ToUri());
			Configuration jobConf = job.GetConfiguration();
			IDictionary<URI, FileStatus> statCache = new Dictionary<URI, FileStatus>();
			ClientDistributedCacheManager.DetermineTimestamps(jobConf, statCache);
			FileStatus firstStatus = statCache[firstCacheFile.ToUri()];
			FileStatus secondStatus = statCache[secondCacheFile.ToUri()];
			NUnit.Framework.Assert.IsNotNull(firstStatus);
			NUnit.Framework.Assert.IsNotNull(secondStatus);
			NUnit.Framework.Assert.AreEqual(2, statCache.Count);
			string expected = firstStatus.GetModificationTime() + "," + secondStatus.GetModificationTime
				();
			NUnit.Framework.Assert.AreEqual(expected, jobConf.Get(MRJobConfig.CacheFileTimestamps
				));
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateTempFile(Path p, Configuration conf)
		{
			SequenceFile.Writer writer = null;
			try
			{
				writer = SequenceFile.CreateWriter(fs, conf, p, typeof(Text), typeof(Text), SequenceFile.CompressionType
					.None);
				writer.Append(new Text("text"), new Text("moretext"));
			}
			catch (Exception e)
			{
				throw new IOException(e.GetLocalizedMessage());
			}
			finally
			{
				if (writer != null)
				{
					writer.Close();
				}
				writer = null;
			}
			Log.Info("created: " + p);
		}
	}
}
