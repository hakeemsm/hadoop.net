using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestShutdownHookManager
	{
		[NUnit.Framework.Test]
		public virtual void ShutdownHookManager()
		{
			Org.Apache.Hadoop.Util.ShutdownHookManager mgr = Org.Apache.Hadoop.Util.ShutdownHookManager
				.Get();
			NUnit.Framework.Assert.IsNotNull(mgr);
			NUnit.Framework.Assert.AreEqual(0, mgr.GetShutdownHooksInOrder().Count);
			Runnable hook1 = new _Runnable_30();
			Runnable hook2 = new _Runnable_35();
			mgr.AddShutdownHook(hook1, 0);
			NUnit.Framework.Assert.IsTrue(mgr.HasShutdownHook(hook1));
			NUnit.Framework.Assert.AreEqual(1, mgr.GetShutdownHooksInOrder().Count);
			NUnit.Framework.Assert.AreEqual(hook1, mgr.GetShutdownHooksInOrder()[0]);
			mgr.RemoveShutdownHook(hook1);
			NUnit.Framework.Assert.IsFalse(mgr.HasShutdownHook(hook1));
			mgr.AddShutdownHook(hook1, 0);
			NUnit.Framework.Assert.IsTrue(mgr.HasShutdownHook(hook1));
			NUnit.Framework.Assert.AreEqual(1, mgr.GetShutdownHooksInOrder().Count);
			NUnit.Framework.Assert.IsTrue(mgr.HasShutdownHook(hook1));
			NUnit.Framework.Assert.AreEqual(1, mgr.GetShutdownHooksInOrder().Count);
			mgr.AddShutdownHook(hook2, 1);
			NUnit.Framework.Assert.IsTrue(mgr.HasShutdownHook(hook1));
			NUnit.Framework.Assert.IsTrue(mgr.HasShutdownHook(hook2));
			NUnit.Framework.Assert.AreEqual(2, mgr.GetShutdownHooksInOrder().Count);
			NUnit.Framework.Assert.AreEqual(hook2, mgr.GetShutdownHooksInOrder()[0]);
			NUnit.Framework.Assert.AreEqual(hook1, mgr.GetShutdownHooksInOrder()[1]);
		}

		private sealed class _Runnable_30 : Runnable
		{
			public _Runnable_30()
			{
			}

			public void Run()
			{
			}
		}

		private sealed class _Runnable_35 : Runnable
		{
			public _Runnable_35()
			{
			}

			public void Run()
			{
			}
		}
	}
}
