using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class TestResult
	{
		// test the PASS value
		public virtual void TestPass()
		{
			Result result = Result.Pass;
			NUnit.Framework.Assert.IsTrue(result.IsPass());
			NUnit.Framework.Assert.IsTrue(result.IsDescend());
		}

		// test the FAIL value
		public virtual void TestFail()
		{
			Result result = Result.Fail;
			NUnit.Framework.Assert.IsFalse(result.IsPass());
			NUnit.Framework.Assert.IsTrue(result.IsDescend());
		}

		// test the STOP value
		public virtual void TestStop()
		{
			Result result = Result.Stop;
			NUnit.Framework.Assert.IsTrue(result.IsPass());
			NUnit.Framework.Assert.IsFalse(result.IsDescend());
		}

		// test combine method with two PASSes
		public virtual void CombinePassPass()
		{
			Result result = Result.Pass.Combine(Result.Pass);
			NUnit.Framework.Assert.IsTrue(result.IsPass());
			NUnit.Framework.Assert.IsTrue(result.IsDescend());
		}

		// test the combine method with a PASS and a FAIL
		public virtual void CombinePassFail()
		{
			Result result = Result.Pass.Combine(Result.Fail);
			NUnit.Framework.Assert.IsFalse(result.IsPass());
			NUnit.Framework.Assert.IsTrue(result.IsDescend());
		}

		// test the combine method with a FAIL and a PASS
		public virtual void CombineFailPass()
		{
			Result result = Result.Fail.Combine(Result.Pass);
			NUnit.Framework.Assert.IsFalse(result.IsPass());
			NUnit.Framework.Assert.IsTrue(result.IsDescend());
		}

		// test the combine method with two FAILs
		public virtual void CombineFailFail()
		{
			Result result = Result.Fail.Combine(Result.Fail);
			NUnit.Framework.Assert.IsFalse(result.IsPass());
			NUnit.Framework.Assert.IsTrue(result.IsDescend());
		}

		// test the combine method with a PASS and STOP
		public virtual void CombinePassStop()
		{
			Result result = Result.Pass.Combine(Result.Stop);
			NUnit.Framework.Assert.IsTrue(result.IsPass());
			NUnit.Framework.Assert.IsFalse(result.IsDescend());
		}

		// test the combine method with a STOP and FAIL
		public virtual void CombineStopFail()
		{
			Result result = Result.Stop.Combine(Result.Fail);
			NUnit.Framework.Assert.IsFalse(result.IsPass());
			NUnit.Framework.Assert.IsFalse(result.IsDescend());
		}

		// test the combine method with a STOP and a PASS
		public virtual void CombineStopPass()
		{
			Result result = Result.Stop.Combine(Result.Pass);
			NUnit.Framework.Assert.IsTrue(result.IsPass());
			NUnit.Framework.Assert.IsFalse(result.IsDescend());
		}

		// test the combine method with a FAIL and a STOP
		public virtual void CombineFailStop()
		{
			Result result = Result.Fail.Combine(Result.Stop);
			NUnit.Framework.Assert.IsFalse(result.IsPass());
			NUnit.Framework.Assert.IsFalse(result.IsDescend());
		}

		// test the negation of PASS
		public virtual void NegatePass()
		{
			Result result = Result.Pass.Negate();
			NUnit.Framework.Assert.IsFalse(result.IsPass());
			NUnit.Framework.Assert.IsTrue(result.IsDescend());
		}

		// test the negation of FAIL
		public virtual void NegateFail()
		{
			Result result = Result.Fail.Negate();
			NUnit.Framework.Assert.IsTrue(result.IsPass());
			NUnit.Framework.Assert.IsTrue(result.IsDescend());
		}

		// test the negation of STOP
		public virtual void NegateStop()
		{
			Result result = Result.Stop.Negate();
			NUnit.Framework.Assert.IsFalse(result.IsPass());
			NUnit.Framework.Assert.IsFalse(result.IsDescend());
		}

		// test equals with two PASSes
		public virtual void EqualsPass()
		{
			Result one = Result.Pass;
			Result two = Result.Pass.Combine(Result.Pass);
			NUnit.Framework.Assert.AreEqual(one, two);
		}

		// test equals with two FAILs
		public virtual void EqualsFail()
		{
			Result one = Result.Fail;
			Result two = Result.Fail.Combine(Result.Fail);
			NUnit.Framework.Assert.AreEqual(one, two);
		}

		// test equals with two STOPS
		public virtual void EqualsStop()
		{
			Result one = Result.Stop;
			Result two = Result.Stop.Combine(Result.Stop);
			NUnit.Framework.Assert.AreEqual(one, two);
		}

		// test all combinations of not equals
		public virtual void NotEquals()
		{
			NUnit.Framework.Assert.IsFalse(Result.Pass.Equals(Result.Fail));
			NUnit.Framework.Assert.IsFalse(Result.Pass.Equals(Result.Stop));
			NUnit.Framework.Assert.IsFalse(Result.Fail.Equals(Result.Pass));
			NUnit.Framework.Assert.IsFalse(Result.Fail.Equals(Result.Stop));
			NUnit.Framework.Assert.IsFalse(Result.Stop.Equals(Result.Pass));
			NUnit.Framework.Assert.IsFalse(Result.Stop.Equals(Result.Fail));
		}
	}
}
