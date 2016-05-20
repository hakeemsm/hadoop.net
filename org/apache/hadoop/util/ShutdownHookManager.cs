using Sharpen;

namespace org.apache.hadoop.util
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
		private static readonly org.apache.hadoop.util.ShutdownHookManager MGR = new org.apache.hadoop.util.ShutdownHookManager
			();

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.ShutdownHookManager
			)));

		static ShutdownHookManager()
		{
			java.lang.Runtime.getRuntime().addShutdownHook(new _Thread_48());
		}

		private sealed class _Thread_48 : java.lang.Thread
		{
			public _Thread_48()
			{
			}

			public override void run()
			{
				org.apache.hadoop.util.ShutdownHookManager.MGR.shutdownInProgress.set(true);
				foreach (java.lang.Runnable hook in org.apache.hadoop.util.ShutdownHookManager.MGR
					.getShutdownHooksInOrder())
				{
					try
					{
						hook.run();
					}
					catch (System.Exception ex)
					{
						org.apache.hadoop.util.ShutdownHookManager.LOG.warn("ShutdownHook '" + Sharpen.Runtime.getClassForObject
							(hook).getSimpleName() + "' failed, " + ex.ToString(), ex);
					}
				}
			}
		}

		/// <summary>Return <code>ShutdownHookManager</code> singleton.</summary>
		/// <returns><code>ShutdownHookManager</code> singleton.</returns>
		public static org.apache.hadoop.util.ShutdownHookManager get()
		{
			return MGR;
		}

		/// <summary>Private structure to store ShutdownHook and its priority.</summary>
		private class HookEntry
		{
			internal java.lang.Runnable hook;

			internal int priority;

			public HookEntry(java.lang.Runnable hook, int priority)
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
					if (obj is org.apache.hadoop.util.ShutdownHookManager.HookEntry)
					{
						eq = (hook == ((org.apache.hadoop.util.ShutdownHookManager.HookEntry)obj).hook);
					}
				}
				return eq;
			}
		}

		private System.Collections.Generic.ICollection<org.apache.hadoop.util.ShutdownHookManager.HookEntry
			> hooks = java.util.Collections.synchronizedSet(new java.util.HashSet<org.apache.hadoop.util.ShutdownHookManager.HookEntry
			>());

		private java.util.concurrent.atomic.AtomicBoolean shutdownInProgress = new java.util.concurrent.atomic.AtomicBoolean
			(false);

		private ShutdownHookManager()
		{
		}

		//private to constructor to ensure singularity
		/// <summary>
		/// Returns the list of shutdownHooks in order of execution,
		/// Highest priority first.
		/// </summary>
		/// <returns>the list of shutdownHooks in order of execution.</returns>
		internal virtual System.Collections.Generic.IList<java.lang.Runnable> getShutdownHooksInOrder
			()
		{
			System.Collections.Generic.IList<org.apache.hadoop.util.ShutdownHookManager.HookEntry
				> list;
			lock (MGR.hooks)
			{
				list = new System.Collections.Generic.List<org.apache.hadoop.util.ShutdownHookManager.HookEntry
					>(MGR.hooks);
			}
			list.Sort(new _Comparator_124());
			//reversing comparison so highest priority hooks are first
			System.Collections.Generic.IList<java.lang.Runnable> ordered = new System.Collections.Generic.List
				<java.lang.Runnable>();
			foreach (org.apache.hadoop.util.ShutdownHookManager.HookEntry entry in list)
			{
				ordered.add(entry.hook);
			}
			return ordered;
		}

		private sealed class _Comparator_124 : java.util.Comparator<org.apache.hadoop.util.ShutdownHookManager.HookEntry
			>
		{
			public _Comparator_124()
			{
			}

			public int compare(org.apache.hadoop.util.ShutdownHookManager.HookEntry o1, org.apache.hadoop.util.ShutdownHookManager.HookEntry
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
		public virtual void addShutdownHook(java.lang.Runnable shutdownHook, int priority
			)
		{
			if (shutdownHook == null)
			{
				throw new System.ArgumentException("shutdownHook cannot be NULL");
			}
			if (shutdownInProgress.get())
			{
				throw new System.InvalidOperationException("Shutdown in progress, cannot add a shutdownHook"
					);
			}
			hooks.add(new org.apache.hadoop.util.ShutdownHookManager.HookEntry(shutdownHook, 
				priority));
		}

		/// <summary>Removes a shutdownHook.</summary>
		/// <param name="shutdownHook">shutdownHook to remove.</param>
		/// <returns>
		/// TRUE if the shutdownHook was registered and removed,
		/// FALSE otherwise.
		/// </returns>
		public virtual bool removeShutdownHook(java.lang.Runnable shutdownHook)
		{
			if (shutdownInProgress.get())
			{
				throw new System.InvalidOperationException("Shutdown in progress, cannot remove a shutdownHook"
					);
			}
			return hooks.remove(new org.apache.hadoop.util.ShutdownHookManager.HookEntry(shutdownHook
				, 0));
		}

		/// <summary>Indicates if a shutdownHook is registered or not.</summary>
		/// <param name="shutdownHook">shutdownHook to check if registered.</param>
		/// <returns>TRUE/FALSE depending if the shutdownHook is is registered.</returns>
		public virtual bool hasShutdownHook(java.lang.Runnable shutdownHook)
		{
			return hooks.contains(new org.apache.hadoop.util.ShutdownHookManager.HookEntry(shutdownHook
				, 0));
		}

		/// <summary>Indicates if shutdown is in progress or not.</summary>
		/// <returns>TRUE if the shutdown is in progress, otherwise FALSE.</returns>
		public virtual bool isShutdownInProgress()
		{
			return shutdownInProgress.get();
		}
	}
}
