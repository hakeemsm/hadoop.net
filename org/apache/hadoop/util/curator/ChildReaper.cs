using Sharpen;

namespace org.apache.hadoop.util.curator
{
	/// <summary>
	/// This is a copy of Curator 2.7.1's ChildReaper class, modified to work with
	/// Guava 11.0.2.
	/// </summary>
	/// <remarks>
	/// This is a copy of Curator 2.7.1's ChildReaper class, modified to work with
	/// Guava 11.0.2.  The problem is the 'paths' Collection, which calls Guava's
	/// Sets.newConcurrentHashSet(), which was added in Guava 15.0.
	/// <p>
	/// Utility to reap empty child nodes of a parent node. Periodically calls getChildren on
	/// the node and adds empty nodes to an internally managed
	/// <see cref="org.apache.curator.framework.recipes.locks.Reaper"/>
	/// </remarks>
	public class ChildReaper : java.io.Closeable
	{
		private readonly org.slf4j.Logger log;

		private readonly org.apache.curator.framework.recipes.locks.Reaper reaper;

		private readonly java.util.concurrent.atomic.AtomicReference<org.apache.hadoop.util.curator.ChildReaper.State
			> state = new java.util.concurrent.atomic.AtomicReference<org.apache.hadoop.util.curator.ChildReaper.State
			>(org.apache.hadoop.util.curator.ChildReaper.State.LATENT);

		private readonly org.apache.curator.framework.CuratorFramework client;

		private readonly System.Collections.Generic.ICollection<string> paths = newConcurrentHashSet
			();

		private readonly org.apache.curator.framework.recipes.locks.Reaper.Mode mode;

		private readonly org.apache.curator.utils.CloseableScheduledExecutorService executor;

		private readonly int reapingThresholdMs;

		private volatile java.util.concurrent.Future<object> task;

		internal static readonly int DEFAULT_REAPING_THRESHOLD_MS = (int)java.util.concurrent.TimeUnit
			.MILLISECONDS.convert(5, java.util.concurrent.TimeUnit.MINUTES);

		// This is copied from Curator's Reaper class
		// This is copied from Guava
		/// <summary>Creates a thread-safe set backed by a hash map.</summary>
		/// <remarks>
		/// Creates a thread-safe set backed by a hash map. The set is backed by a
		/// <see cref="java.util.concurrent.ConcurrentHashMap{K, V}"/>
		/// instance, and thus carries the same concurrency
		/// guarantees.
		/// <p>Unlike
		/// <c>HashSet</c>
		/// , this class does NOT allow
		/// <see langword="null"/>
		/// to be
		/// used as an element. The set is serializable.
		/// </remarks>
		/// <returns>
		/// a new, empty thread-safe
		/// <c>Set</c>
		/// </returns>
		/// <since>15.0</since>
		public static System.Collections.Generic.ICollection<E> newConcurrentHashSet<E>()
		{
			return com.google.common.collect.Sets.newSetFromMap(new java.util.concurrent.ConcurrentHashMap
				<E, bool>());
		}

		private enum State
		{
			LATENT,
			STARTED,
			CLOSED
		}

		/// <param name="client">the client</param>
		/// <param name="path">path to reap children from</param>
		/// <param name="mode">reaping mode</param>
		public ChildReaper(org.apache.curator.framework.CuratorFramework client, string path
			, org.apache.curator.framework.recipes.locks.Reaper.Mode mode)
			: this(client, path, mode, newExecutorService(), DEFAULT_REAPING_THRESHOLD_MS, null
				)
		{
			log = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForObject(this));
		}

		/// <param name="client">the client</param>
		/// <param name="path">path to reap children from</param>
		/// <param name="reapingThresholdMs">threshold in milliseconds that determines that a path can be deleted
		/// 	</param>
		/// <param name="mode">reaping mode</param>
		public ChildReaper(org.apache.curator.framework.CuratorFramework client, string path
			, org.apache.curator.framework.recipes.locks.Reaper.Mode mode, int reapingThresholdMs
			)
			: this(client, path, mode, newExecutorService(), reapingThresholdMs, null)
		{
			log = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForObject(this));
		}

		/// <param name="client">the client</param>
		/// <param name="path">path to reap children from</param>
		/// <param name="executor">executor to use for background tasks</param>
		/// <param name="reapingThresholdMs">threshold in milliseconds that determines that a path can be deleted
		/// 	</param>
		/// <param name="mode">reaping mode</param>
		public ChildReaper(org.apache.curator.framework.CuratorFramework client, string path
			, org.apache.curator.framework.recipes.locks.Reaper.Mode mode, java.util.concurrent.ScheduledExecutorService
			 executor, int reapingThresholdMs)
			: this(client, path, mode, executor, reapingThresholdMs, null)
		{
			log = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForObject(this));
		}

		/// <param name="client">the client</param>
		/// <param name="path">path to reap children from</param>
		/// <param name="executor">executor to use for background tasks</param>
		/// <param name="reapingThresholdMs">threshold in milliseconds that determines that a path can be deleted
		/// 	</param>
		/// <param name="mode">reaping mode</param>
		/// <param name="leaderPath">if not null, uses a leader selection so that only 1 reaper is active in the cluster
		/// 	</param>
		public ChildReaper(org.apache.curator.framework.CuratorFramework client, string path
			, org.apache.curator.framework.recipes.locks.Reaper.Mode mode, java.util.concurrent.ScheduledExecutorService
			 executor, int reapingThresholdMs, string leaderPath)
		{
			log = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForObject(this));
			this.client = client;
			this.mode = mode;
			this.executor = new org.apache.curator.utils.CloseableScheduledExecutorService(executor
				);
			this.reapingThresholdMs = reapingThresholdMs;
			this.reaper = new org.apache.curator.framework.recipes.locks.Reaper(client, executor
				, reapingThresholdMs, leaderPath);
			addPath(path);
		}

		/// <summary>The reaper must be started</summary>
		/// <exception cref="System.Exception">errors</exception>
		public virtual void start()
		{
			com.google.common.@base.Preconditions.checkState(state.compareAndSet(org.apache.hadoop.util.curator.ChildReaper.State
				.LATENT, org.apache.hadoop.util.curator.ChildReaper.State.STARTED), "Cannot be started more than once"
				);
			task = executor.scheduleWithFixedDelay(new _Runnable_158(this), reapingThresholdMs
				, reapingThresholdMs, java.util.concurrent.TimeUnit.MILLISECONDS);
			reaper.start();
		}

		private sealed class _Runnable_158 : java.lang.Runnable
		{
			public _Runnable_158(ChildReaper _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void run()
			{
				this._enclosing.doWork();
			}

			private readonly ChildReaper _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			if (state.compareAndSet(org.apache.hadoop.util.curator.ChildReaper.State.STARTED, 
				org.apache.hadoop.util.curator.ChildReaper.State.CLOSED))
			{
				org.apache.curator.utils.CloseableUtils.closeQuietly(reaper);
				task.cancel(true);
			}
		}

		/// <summary>Add a path to reap children from</summary>
		/// <param name="path">the path</param>
		/// <returns>this for chaining</returns>
		public virtual org.apache.hadoop.util.curator.ChildReaper addPath(string path)
		{
			paths.add(org.apache.curator.utils.PathUtils.validatePath(path));
			return this;
		}

		/// <summary>Remove a path from reaping</summary>
		/// <param name="path">the path</param>
		/// <returns>true if the path existed and was removed</returns>
		public virtual bool removePath(string path)
		{
			return paths.remove(org.apache.curator.utils.PathUtils.validatePath(path));
		}

		private static java.util.concurrent.ScheduledExecutorService newExecutorService()
		{
			return org.apache.curator.utils.ThreadUtils.newFixedThreadScheduledPool(2, "ChildReaper"
				);
		}

		private void doWork()
		{
			foreach (string path in paths)
			{
				try
				{
					System.Collections.Generic.IList<string> children = client.getChildren().forPath(
						path);
					foreach (string name in children)
					{
						string thisPath = org.apache.curator.utils.ZKPaths.makePath(path, name);
						org.apache.zookeeper.data.Stat stat = client.checkExists().forPath(thisPath);
						if ((stat != null) && (stat.getNumChildren() == 0))
						{
							reaper.addPath(thisPath, mode);
						}
					}
				}
				catch (System.Exception e)
				{
					log.error("Could not get children for path: " + path, e);
				}
			}
		}
	}
}
