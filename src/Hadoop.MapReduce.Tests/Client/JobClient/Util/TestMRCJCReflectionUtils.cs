using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Test for the JobConf-related parts of common's ReflectionUtils
	/// class.
	/// </summary>
	public class TestMRCJCReflectionUtils
	{
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			ReflectionUtils.ClearCache();
		}

		/// <summary>
		/// This is to test backward compatibility of ReflectionUtils for
		/// JobConfigurable objects.
		/// </summary>
		/// <remarks>
		/// This is to test backward compatibility of ReflectionUtils for
		/// JobConfigurable objects.
		/// This should be made deprecated along with the mapred package HADOOP-1230.
		/// Should be removed when mapred package is removed.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestSetConf()
		{
			TestMRCJCReflectionUtils.JobConfigurableOb ob = new TestMRCJCReflectionUtils.JobConfigurableOb
				();
			ReflectionUtils.SetConf(ob, new Configuration());
			NUnit.Framework.Assert.IsFalse(ob.configured);
			ReflectionUtils.SetConf(ob, new JobConf());
			NUnit.Framework.Assert.IsTrue(ob.configured);
		}

		private class JobConfigurableOb : JobConfigurable
		{
			internal bool configured;

			public virtual void Configure(JobConf job)
			{
				configured = true;
			}
		}
	}
}
