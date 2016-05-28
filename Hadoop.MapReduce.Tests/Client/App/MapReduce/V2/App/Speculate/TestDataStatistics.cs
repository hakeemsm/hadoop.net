using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	public class TestDataStatistics
	{
		private const double Tol = 0.001;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyDataStatistics()
		{
			DataStatistics statistics = new DataStatistics();
			NUnit.Framework.Assert.AreEqual(0, statistics.Count(), Tol);
			NUnit.Framework.Assert.AreEqual(0, statistics.Mean(), Tol);
			NUnit.Framework.Assert.AreEqual(0, statistics.Var(), Tol);
			NUnit.Framework.Assert.AreEqual(0, statistics.Std(), Tol);
			NUnit.Framework.Assert.AreEqual(0, statistics.Outlier(1.0f), Tol);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleEntryDataStatistics()
		{
			DataStatistics statistics = new DataStatistics(17.29);
			NUnit.Framework.Assert.AreEqual(1, statistics.Count(), Tol);
			NUnit.Framework.Assert.AreEqual(17.29, statistics.Mean(), Tol);
			NUnit.Framework.Assert.AreEqual(0, statistics.Var(), Tol);
			NUnit.Framework.Assert.AreEqual(0, statistics.Std(), Tol);
			NUnit.Framework.Assert.AreEqual(17.29, statistics.Outlier(1.0f), Tol);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMutiEntryDataStatistics()
		{
			DataStatistics statistics = new DataStatistics();
			statistics.Add(17);
			statistics.Add(29);
			NUnit.Framework.Assert.AreEqual(2, statistics.Count(), Tol);
			NUnit.Framework.Assert.AreEqual(23.0, statistics.Mean(), Tol);
			NUnit.Framework.Assert.AreEqual(36.0, statistics.Var(), Tol);
			NUnit.Framework.Assert.AreEqual(6.0, statistics.Std(), Tol);
			NUnit.Framework.Assert.AreEqual(29.0, statistics.Outlier(1.0f), Tol);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateStatistics()
		{
			DataStatistics statistics = new DataStatistics(17);
			statistics.Add(29);
			NUnit.Framework.Assert.AreEqual(2, statistics.Count(), Tol);
			NUnit.Framework.Assert.AreEqual(23.0, statistics.Mean(), Tol);
			NUnit.Framework.Assert.AreEqual(36.0, statistics.Var(), Tol);
			statistics.UpdateStatistics(17, 29);
			NUnit.Framework.Assert.AreEqual(2, statistics.Count(), Tol);
			NUnit.Framework.Assert.AreEqual(29.0, statistics.Mean(), Tol);
			NUnit.Framework.Assert.AreEqual(0.0, statistics.Var(), Tol);
		}
	}
}
