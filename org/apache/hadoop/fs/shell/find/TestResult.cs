using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	public class TestResult
	{
		// test the PASS value
		public virtual void testPass()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.PASS;
			NUnit.Framework.Assert.IsTrue(result.isPass());
			NUnit.Framework.Assert.IsTrue(result.isDescend());
		}

		// test the FAIL value
		public virtual void testFail()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.FAIL;
			NUnit.Framework.Assert.IsFalse(result.isPass());
			NUnit.Framework.Assert.IsTrue(result.isDescend());
		}

		// test the STOP value
		public virtual void testStop()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.STOP;
			NUnit.Framework.Assert.IsTrue(result.isPass());
			NUnit.Framework.Assert.IsFalse(result.isDescend());
		}

		// test combine method with two PASSes
		public virtual void combinePassPass()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.PASS.combine(org.apache.hadoop.fs.shell.find.Result.PASS);
			NUnit.Framework.Assert.IsTrue(result.isPass());
			NUnit.Framework.Assert.IsTrue(result.isDescend());
		}

		// test the combine method with a PASS and a FAIL
		public virtual void combinePassFail()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.PASS.combine(org.apache.hadoop.fs.shell.find.Result.FAIL);
			NUnit.Framework.Assert.IsFalse(result.isPass());
			NUnit.Framework.Assert.IsTrue(result.isDescend());
		}

		// test the combine method with a FAIL and a PASS
		public virtual void combineFailPass()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.FAIL.combine(org.apache.hadoop.fs.shell.find.Result.PASS);
			NUnit.Framework.Assert.IsFalse(result.isPass());
			NUnit.Framework.Assert.IsTrue(result.isDescend());
		}

		// test the combine method with two FAILs
		public virtual void combineFailFail()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.FAIL.combine(org.apache.hadoop.fs.shell.find.Result.FAIL);
			NUnit.Framework.Assert.IsFalse(result.isPass());
			NUnit.Framework.Assert.IsTrue(result.isDescend());
		}

		// test the combine method with a PASS and STOP
		public virtual void combinePassStop()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.PASS.combine(org.apache.hadoop.fs.shell.find.Result.STOP);
			NUnit.Framework.Assert.IsTrue(result.isPass());
			NUnit.Framework.Assert.IsFalse(result.isDescend());
		}

		// test the combine method with a STOP and FAIL
		public virtual void combineStopFail()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.STOP.combine(org.apache.hadoop.fs.shell.find.Result.FAIL);
			NUnit.Framework.Assert.IsFalse(result.isPass());
			NUnit.Framework.Assert.IsFalse(result.isDescend());
		}

		// test the combine method with a STOP and a PASS
		public virtual void combineStopPass()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.STOP.combine(org.apache.hadoop.fs.shell.find.Result.PASS);
			NUnit.Framework.Assert.IsTrue(result.isPass());
			NUnit.Framework.Assert.IsFalse(result.isDescend());
		}

		// test the combine method with a FAIL and a STOP
		public virtual void combineFailStop()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.FAIL.combine(org.apache.hadoop.fs.shell.find.Result.STOP);
			NUnit.Framework.Assert.IsFalse(result.isPass());
			NUnit.Framework.Assert.IsFalse(result.isDescend());
		}

		// test the negation of PASS
		public virtual void negatePass()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.PASS.negate();
			NUnit.Framework.Assert.IsFalse(result.isPass());
			NUnit.Framework.Assert.IsTrue(result.isDescend());
		}

		// test the negation of FAIL
		public virtual void negateFail()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.FAIL.negate();
			NUnit.Framework.Assert.IsTrue(result.isPass());
			NUnit.Framework.Assert.IsTrue(result.isDescend());
		}

		// test the negation of STOP
		public virtual void negateStop()
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.STOP.negate();
			NUnit.Framework.Assert.IsFalse(result.isPass());
			NUnit.Framework.Assert.IsFalse(result.isDescend());
		}

		// test equals with two PASSes
		public virtual void equalsPass()
		{
			org.apache.hadoop.fs.shell.find.Result one = org.apache.hadoop.fs.shell.find.Result
				.PASS;
			org.apache.hadoop.fs.shell.find.Result two = org.apache.hadoop.fs.shell.find.Result
				.PASS.combine(org.apache.hadoop.fs.shell.find.Result.PASS);
			NUnit.Framework.Assert.AreEqual(one, two);
		}

		// test equals with two FAILs
		public virtual void equalsFail()
		{
			org.apache.hadoop.fs.shell.find.Result one = org.apache.hadoop.fs.shell.find.Result
				.FAIL;
			org.apache.hadoop.fs.shell.find.Result two = org.apache.hadoop.fs.shell.find.Result
				.FAIL.combine(org.apache.hadoop.fs.shell.find.Result.FAIL);
			NUnit.Framework.Assert.AreEqual(one, two);
		}

		// test equals with two STOPS
		public virtual void equalsStop()
		{
			org.apache.hadoop.fs.shell.find.Result one = org.apache.hadoop.fs.shell.find.Result
				.STOP;
			org.apache.hadoop.fs.shell.find.Result two = org.apache.hadoop.fs.shell.find.Result
				.STOP.combine(org.apache.hadoop.fs.shell.find.Result.STOP);
			NUnit.Framework.Assert.AreEqual(one, two);
		}

		// test all combinations of not equals
		public virtual void notEquals()
		{
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.shell.find.Result.PASS.Equals
				(org.apache.hadoop.fs.shell.find.Result.FAIL));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.shell.find.Result.PASS.Equals
				(org.apache.hadoop.fs.shell.find.Result.STOP));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.shell.find.Result.FAIL.Equals
				(org.apache.hadoop.fs.shell.find.Result.PASS));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.shell.find.Result.FAIL.Equals
				(org.apache.hadoop.fs.shell.find.Result.STOP));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.shell.find.Result.STOP.Equals
				(org.apache.hadoop.fs.shell.find.Result.PASS));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.shell.find.Result.STOP.Equals
				(org.apache.hadoop.fs.shell.find.Result.FAIL));
		}
	}
}
