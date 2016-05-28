using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>test SkipBadRecords</summary>
	public class TestSkipBadRecords
	{
		public virtual void TestSkipBadRecords()
		{
			// test default values
			Configuration conf = new Configuration();
			NUnit.Framework.Assert.AreEqual(2, SkipBadRecords.GetAttemptsToStartSkipping(conf
				));
			NUnit.Framework.Assert.IsTrue(SkipBadRecords.GetAutoIncrMapperProcCount(conf));
			NUnit.Framework.Assert.IsTrue(SkipBadRecords.GetAutoIncrReducerProcCount(conf));
			NUnit.Framework.Assert.AreEqual(0, SkipBadRecords.GetMapperMaxSkipRecords(conf));
			NUnit.Framework.Assert.AreEqual(0, SkipBadRecords.GetReducerMaxSkipGroups(conf), 
				0);
			NUnit.Framework.Assert.IsNull(SkipBadRecords.GetSkipOutputPath(conf));
			// test setters
			SkipBadRecords.SetAttemptsToStartSkipping(conf, 5);
			SkipBadRecords.SetAutoIncrMapperProcCount(conf, false);
			SkipBadRecords.SetAutoIncrReducerProcCount(conf, false);
			SkipBadRecords.SetMapperMaxSkipRecords(conf, 6L);
			SkipBadRecords.SetReducerMaxSkipGroups(conf, 7L);
			JobConf jc = new JobConf();
			SkipBadRecords.SetSkipOutputPath(jc, new Path("test"));
			// test getters 
			NUnit.Framework.Assert.AreEqual(5, SkipBadRecords.GetAttemptsToStartSkipping(conf
				));
			NUnit.Framework.Assert.IsFalse(SkipBadRecords.GetAutoIncrMapperProcCount(conf));
			NUnit.Framework.Assert.IsFalse(SkipBadRecords.GetAutoIncrReducerProcCount(conf));
			NUnit.Framework.Assert.AreEqual(6L, SkipBadRecords.GetMapperMaxSkipRecords(conf));
			NUnit.Framework.Assert.AreEqual(7L, SkipBadRecords.GetReducerMaxSkipGroups(conf), 
				0);
			NUnit.Framework.Assert.AreEqual("test", SkipBadRecords.GetSkipOutputPath(jc).ToString
				());
		}
	}
}
