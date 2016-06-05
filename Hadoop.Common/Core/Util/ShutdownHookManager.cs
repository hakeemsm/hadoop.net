using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// The <code>ShutdownHookManager</code> enables running shutdownHook
	/// in a deterministic order, higher priority first.
	/// </summary>
	/// <remarks>
	/// The <code>ShutdownHookManager</code> enables running shutdownHook
	/// in a deterministic order, higher priority first.
	/// <p/>
	/// The JVM runs ShutdownHooks in a non-deterministic order or in parallel.
	/// This class registers a single JVM shutdownHook and run all the
	/// shutdownHooks registered to it (to this class) in order based on their
	/// priority.
	/// </remarks>
	public class ShutdownHookManager
	{
		private static readonly Org.Apache.Hadoop.Util.ShutdownHookManager Mgr = new Org.Apache.Hadoop.Util.ShutdownHookManager
			();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.ShutdownHookManager
			));

		static ShutdownHookManager()
		{
			Runtime.GetRuntime().AddShutdownHook(new _Thread_48());
		}

		private sealed class _Thread_48 : Thread
		{
			public _Thread_48()
			{
			}

			public override void Run()
			{
				Org.Apache.Hadoop.Util.ShutdownHookManager.Mgr.shutdownInProgress.Set(true);
				foreach (Runnable hook in Org.Apache.Hadoop.Util.ShutdownHookManager.Mgr.GetShutdownHooksInOrder
					())
				{
					try
					{
						hook.Run();
					}
					catch (Exception ex)
					{
						Org.Apache.Hadoop.Util.ShutdownHookManager.Log.Warn("ShutdownHook '" + hook.GetType
							().Name + "' failed, " + ex.ToString(), ex);
					}
				}
			}
		}

		/// <summary>Return <code>ShutdownHookManager</code> singleton.</summary>
		/// <returns><code>ShutdownHookManager</code> singleton.</returns>
		public static Org.Apache.Hadoop.Util.ShutdownHookManager Get()
		{
			return Mgr;
		}

		/// <summary>Private structure to store ShutdownHook and its priority.</summary>
		private class HookEntry
		{
			internal Runnable hook;

			internal int priority;

			public HookEntry(Runnable hook, int priority)
			{
				this.hook = hook;
				this.priority = priority;
			}

			public override int GetHashCode()
			{
				return hook.GetHashCode();
			}

			public override bool Equals(object obj)
			{
				bool eq = false;
				if (obj != null)
				{
					if (obj is ShutdownHookManager.HookEntry)
					{
						eq = (hook == ((ShutdownHookManager.HookEntry)obj).hook);
					}
				}
				return eq;
			}
		}

		private ICollection<ShutdownHookManager.HookEntry> hooks = Collections.SynchronizedSet
			(new HashSet<ShutdownHookManager.HookEntry>());

		private AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

		private ShutdownHookManager()
		{
		}

		//private to constructor to ensure singularity
		/// <summary>
		/// Returns the list of shutdownHooks in order of execution,
		/// Highest priority first.
		/// </summary>
		/// <returns>the list of shutdownHooks in order of execution.</returns>
		internal virtual IList<Runnable> GetShutdownHooksInOrder()
		{
			IList<ShutdownHookManager.HookEntry> list;
			lock (Mgr.hooks)
			{
				list = new AList<ShutdownHookManager.HookEntry>(Mgr.hooks);
			}
			list.Sort(new _IComparer_124());
			//reversing comparison so highest priority hooks are first
			IList<Runnable> ordered = new AList<Runnable>();
			foreach (ShutdownHookManager.HookEntry entry in list)
			{
				ordered.AddItem(entry.hook);
			}
			return ordered;
		}

		private sealed class _IComparer_124 : IComparer<ShutdownHookManager.HookEntry>
		{
			public _IComparer_124()
			{
			}

			public int Compare(ShutdownHookManager.HookEntry o1, ShutdownHookManager.HookEntry
				 o2)
			{
				return o2.priority - o1.priority;
			}
		}

		/// <summary>
		/// Adds a shutdownHook with a priority, the higher the priority
		/// the earlier will run.
		/// </summary>
		/// <remarks>
		/// Adds a shutdownHook with a priority, the higher the priority
		/// the earlier will run. ShutdownHooks with same priority run
		/// in a non-deterministic order.
		/// </remarks>
		/// <param name="shutdownHook">shutdownHook <code>Runnable</code></param>
		/// <param name="priority">priority of the shutdownHook.</param>
		public virtual void AddShutdownHook(Runnable shutdownHook, int priority)
		{
			if (shutdownHook == null)
			{
				throw new ArgumentException("shutdownHook cannot be NULL");
			}
			if (shutdownInProgress.Get())
			{
				throw new InvalidOperationException("Shutdown in progress, cannot add a shutdownHook"
					);
			}
			hooks.AddItem(new ShutdownHookManager.HookEntry(shutdownHook, priority));
		}

		/// <summary>Removes a shutdownHook.</summary>
		/// <param name="shutdownHook">shutdownHook to remove.</param>
		/// <returns>
		/// TRUE if the shutdownHook was registered and removed,
		/// FALSE otherwise.
		/// </returns>
		public virtual bool RemoveShutdownHook(Runnable shutdownHook)
		{
			if (shutdownInProgress.Get())
			{
				throw new InvalidOperationException("Shutdown in progress, cannot remove a shutdownHook"
					);
			}
			return hooks.Remove(new ShutdownHookManager.HookEntry(shutdownHook, 0));
		}

		/// <summary>Indicates if a shutdownHook is registered or not.</summary>
		/// <param name="shutdownHook">shutdownHook to check if registered.</param>
		/// <returns>TRUE/FALSE depending if the shutdownHook is is registered.</returns>
		public virtual bool HasShutdownHook(Runnable shutdownHook)
		{
			return hooks.Contains(new ShutdownHookManager.HookEntry(shutdownHook, 0));
		}

		/// <summary>Indicates if shutdown is in progress or not.</summary>
		/// <returns>TRUE if the shutdown is in progress, otherwise FALSE.</returns>
		public virtual bool IsShutdownInProgress()
		{
			return shutdownInProgress.Get();
		}
	}
}
