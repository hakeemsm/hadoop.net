using NUnit.Framework;


namespace Org.Apache.Hadoop.Util
{
	public class TestShutdownHookManager
	{
		[Fact]
		public virtual void ShutdownHookManager()
		{
			Org.Apache.Hadoop.Util.ShutdownHookManager mgr = Org.Apache.Hadoop.Util.ShutdownHookManager
				.Get();
			NUnit.Framework.Assert.IsNotNull(mgr);
			Assert.Equal(0, mgr.GetShutdownHooksInOrder().Count);
			Runnable hook1 = new _Runnable_30();
			Runnable hook2 = new _Runnable_35();
			mgr.AddShutdownHook(hook1, 0);
			Assert.True(mgr.HasShutdownHook(hook1));
			Assert.Equal(1, mgr.GetShutdownHooksInOrder().Count);
			Assert.Equal(hook1, mgr.GetShutdownHooksInOrder()[0]);
			mgr.RemoveShutdownHook(hook1);
			NUnit.Framework.Assert.IsFalse(mgr.HasShutdownHook(hook1));
			mgr.AddShutdownHook(hook1, 0);
			Assert.True(mgr.HasShutdownHook(hook1));
			Assert.Equal(1, mgr.GetShutdownHooksInOrder().Count);
			Assert.True(mgr.HasShutdownHook(hook1));
			Assert.Equal(1, mgr.GetShutdownHooksInOrder().Count);
			mgr.AddShutdownHook(hook2, 1);
			Assert.True(mgr.HasShutdownHook(hook1));
			Assert.True(mgr.HasShutdownHook(hook2));
			Assert.Equal(2, mgr.GetShutdownHooksInOrder().Count);
			Assert.Equal(hook2, mgr.GetShutdownHooksInOrder()[0]);
			Assert.Equal(hook1, mgr.GetShutdownHooksInOrder()[1]);
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
