using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestUtils
	{
		private static readonly Path[] LogPaths = new Path[] { new Path("file:///foo/_logs"
			), new Path("file:///foo/_logs/"), new Path("_logs/"), new Path("_logs") };

		private static readonly Path[] SucceededPaths = new Path[] { new Path("file:///blah/"
			 + FileOutputCommitter.SucceededFileName) };

		private static readonly Path[] PassPaths = new Path[] { new Path("file:///my_logs/blah"
			), new Path("file:///a/b/c"), new Path("file:///foo/_logs/blah"), new Path("_logs/foo"
			), new Path("file:///blah/" + FileOutputCommitter.SucceededFileName + "/bar") };

		[NUnit.Framework.Test]
		public virtual void TestOutputFilesFilter()
		{
			PathFilter filter = new Utils.OutputFileUtils.OutputFilesFilter();
			foreach (Path p in LogPaths)
			{
				NUnit.Framework.Assert.IsFalse(filter.Accept(p));
			}
			foreach (Path p_1 in SucceededPaths)
			{
				NUnit.Framework.Assert.IsFalse(filter.Accept(p_1));
			}
			foreach (Path p_2 in PassPaths)
			{
				NUnit.Framework.Assert.IsTrue(filter.Accept(p_2));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestLogFilter()
		{
			PathFilter filter = new Utils.OutputFileUtils.OutputLogFilter();
			foreach (Path p in LogPaths)
			{
				NUnit.Framework.Assert.IsFalse(filter.Accept(p));
			}
			foreach (Path p_1 in SucceededPaths)
			{
				NUnit.Framework.Assert.IsTrue(filter.Accept(p_1));
			}
			foreach (Path p_2 in PassPaths)
			{
				NUnit.Framework.Assert.IsTrue(filter.Accept(p_2));
			}
		}
	}
}
