using System;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Lang
{
	public class TestRunnableCallable : HTestCase
	{
		public class R : Runnable
		{
			internal bool Run;

			public virtual void Run()
			{
				Run = true;
			}
		}

		public class C : Callable
		{
			internal bool Run;

			/// <exception cref="System.Exception"/>
			public virtual object Call()
			{
				Run = true;
				return null;
			}
		}

		public class CEx : Callable
		{
			/// <exception cref="System.Exception"/>
			public virtual object Call()
			{
				throw new Exception();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Runnable()
		{
			TestRunnableCallable.R r = new TestRunnableCallable.R();
			RunnableCallable rc = new RunnableCallable(r);
			rc.Run();
			NUnit.Framework.Assert.IsTrue(r.Run);
			r = new TestRunnableCallable.R();
			rc = new RunnableCallable(r);
			rc.Call();
			NUnit.Framework.Assert.IsTrue(r.Run);
			NUnit.Framework.Assert.AreEqual(rc.ToString(), "R");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Callable()
		{
			TestRunnableCallable.C c = new TestRunnableCallable.C();
			RunnableCallable rc = new RunnableCallable(c);
			rc.Run();
			NUnit.Framework.Assert.IsTrue(c.Run);
			c = new TestRunnableCallable.C();
			rc = new RunnableCallable(c);
			rc.Call();
			NUnit.Framework.Assert.IsTrue(c.Run);
			NUnit.Framework.Assert.AreEqual(rc.ToString(), "C");
		}

		/// <exception cref="System.Exception"/>
		public virtual void CallableExRun()
		{
			TestRunnableCallable.CEx c = new TestRunnableCallable.CEx();
			RunnableCallable rc = new RunnableCallable(c);
			rc.Run();
		}
	}
}
