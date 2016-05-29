using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestApplicationAttemptId
	{
		[NUnit.Framework.Test]
		public virtual void TestApplicationAttemptId()
		{
			ApplicationAttemptId a1 = CreateAppAttemptId(10l, 1, 1);
			ApplicationAttemptId a2 = CreateAppAttemptId(10l, 1, 2);
			ApplicationAttemptId a3 = CreateAppAttemptId(10l, 2, 1);
			ApplicationAttemptId a4 = CreateAppAttemptId(8l, 1, 4);
			ApplicationAttemptId a5 = CreateAppAttemptId(10l, 1, 1);
			NUnit.Framework.Assert.IsTrue(a1.Equals(a5));
			NUnit.Framework.Assert.IsFalse(a1.Equals(a2));
			NUnit.Framework.Assert.IsFalse(a1.Equals(a3));
			NUnit.Framework.Assert.IsFalse(a1.Equals(a4));
			NUnit.Framework.Assert.IsTrue(a1.CompareTo(a5) == 0);
			NUnit.Framework.Assert.IsTrue(a1.CompareTo(a2) < 0);
			NUnit.Framework.Assert.IsTrue(a1.CompareTo(a3) < 0);
			NUnit.Framework.Assert.IsTrue(a1.CompareTo(a4) > 0);
			NUnit.Framework.Assert.IsTrue(a1.GetHashCode() == a5.GetHashCode());
			NUnit.Framework.Assert.IsFalse(a1.GetHashCode() == a2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(a1.GetHashCode() == a3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(a1.GetHashCode() == a4.GetHashCode());
			long ts = Runtime.CurrentTimeMillis();
			ApplicationAttemptId a6 = CreateAppAttemptId(ts, 543627, 33492611);
			NUnit.Framework.Assert.AreEqual("appattempt_10_0001_000001", a1.ToString());
			NUnit.Framework.Assert.AreEqual("appattempt_" + ts + "_543627_33492611", a6.ToString
				());
		}

		private ApplicationAttemptId CreateAppAttemptId(long clusterTimeStamp, int id, int
			 attemptId)
		{
			ApplicationId appId = ApplicationId.NewInstance(clusterTimeStamp, id);
			return ApplicationAttemptId.NewInstance(appId, attemptId);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Org.Apache.Hadoop.Yarn.Api.TestApplicationAttemptId t = new Org.Apache.Hadoop.Yarn.Api.TestApplicationAttemptId
				();
			t.TestApplicationAttemptId();
		}
	}
}
