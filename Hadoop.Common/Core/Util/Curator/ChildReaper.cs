using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Curator.Framework;
using Org.Apache.Curator.Framework.Recipes.Locks;
using Org.Apache.Curator.Utils;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;


namespace Org.Apache.Hadoop.Util.Curator
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
	/// <see cref="Org.Apache.Curator.Framework.Recipes.Locks.Reaper"/>
	/// </remarks>
	public class ChildReaper : IDisposable
	{
		private readonly Logger log = LoggerFactory.GetLogger(GetType());

		private readonly Reaper reaper;

		private readonly AtomicReference<ChildReaper.State> state = new AtomicReference<ChildReaper.State
			>(ChildReaper.State.Latent);

		private readonly CuratorFramework client;

		private readonly ICollection<string> paths = NewConcurrentHashSet();

		private readonly Reaper.Mode mode;

		private readonly CloseableScheduledExecutorService executor;

		private readonly int reapingThresholdMs;

		private volatile Future<object> task;

		internal static readonly int DefaultReapingThresholdMs = (int)TimeUnit.Milliseconds
			.Convert(5, TimeUnit.Minutes);

		// This is copied from Curator's Reaper class
		// This is copied from Guava
		/// <summary>Creates a thread-safe set backed by a hash map.</summary>
		/// <remarks>
		/// Creates a thread-safe set backed by a hash map. The set is backed by a
		/// <see cref="ConcurrentHashMap{K, V}"/>
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
		public static ICollection<E> NewConcurrentHashSet<E>()
		{
			return Sets.NewSetFromMap(new ConcurrentHashMap<E, bool>());
		}

		private enum State
		{
			Latent,
			Started,
			Closed
		}

		/// <param name="client">the client</param>
		/// <param name="path">path to reap children from</param>
		/// <param name="mode">reaping mode</param>
		public ChildReaper(CuratorFramework client, string path, Reaper.Mode mode)
			: this(client, path, mode, NewExecutorService(), DefaultReapingThresholdMs, null)
		{
		}

		/// <param name="client">the client</param>
		/// <param name="path">path to reap children from</param>
		/// <param name="reapingThresholdMs">threshold in milliseconds that determines that a path can be deleted
		/// 	</param>
		/// <param name="mode">reaping mode</param>
		public ChildReaper(CuratorFramework client, string path, Reaper.Mode mode, int reapingThresholdMs
			)
			: this(client, path, mode, NewExecutorService(), reapingThresholdMs, null)
		{
		}

		/// <param name="client">the client</param>
		/// <param name="path">path to reap children from</param>
		/// <param name="executor">executor to use for background tasks</param>
		/// <param name="reapingThresholdMs">threshold in milliseconds that determines that a path can be deleted
		/// 	</param>
		/// <param name="mode">reaping mode</param>
		public ChildReaper(CuratorFramework client, string path, Reaper.Mode mode, ScheduledExecutorService
			 executor, int reapingThresholdMs)
			: this(client, path, mode, executor, reapingThresholdMs, null)
		{
		}

		/// <param name="client">the client</param>
		/// <param name="path">path to reap children from</param>
		/// <param name="executor">executor to use for background tasks</param>
		/// <param name="reapingThresholdMs">threshold in milliseconds that determines that a path can be deleted
		/// 	</param>
		/// <param name="mode">reaping mode</param>
		/// <param name="leaderPath">if not null, uses a leader selection so that only 1 reaper is active in the cluster
		/// 	</param>
		public ChildReaper(CuratorFramework client, string path, Reaper.Mode mode, ScheduledExecutorService
			 executor, int reapingThresholdMs, string leaderPath)
		{
			this.client = client;
			this.mode = mode;
			this.executor = new CloseableScheduledExecutorService(executor);
			this.reapingThresholdMs = reapingThresholdMs;
			this.reaper = new Reaper(client, executor, reapingThresholdMs, leaderPath);
			AddPath(path);
		}

		/// <summary>The reaper must be started</summary>
		/// <exception cref="System.Exception">errors</exception>
		public virtual void Start()
		{
			Preconditions.CheckState(state.CompareAndSet(ChildReaper.State.Latent, ChildReaper.State
				.Started), "Cannot be started more than once");
			task = executor.ScheduleWithFixedDelay(new _Runnable_158(this), reapingThresholdMs
				, reapingThresholdMs, TimeUnit.Milliseconds);
			reaper.Start();
		}

		private sealed class _Runnable_158 : Runnable
		{
			public _Runnable_158(ChildReaper _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				this._enclosing.DoWork();
			}

			private readonly ChildReaper _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			if (state.CompareAndSet(ChildReaper.State.Started, ChildReaper.State.Closed))
			{
				CloseableUtils.CloseQuietly(reaper);
				task.Cancel(true);
			}
		}

		/// <summary>Add a path to reap children from</summary>
		/// <param name="path">the path</param>
		/// <returns>this for chaining</returns>
		public virtual Org.Apache.Hadoop.Util.Curator.ChildReaper AddPath(string path)
		{
			paths.AddItem(PathUtils.ValidatePath(path));
			return this;
		}

		/// <summary>Remove a path from reaping</summary>
		/// <param name="path">the path</param>
		/// <returns>true if the path existed and was removed</returns>
		public virtual bool RemovePath(string path)
		{
			return paths.Remove(PathUtils.ValidatePath(path));
		}

		private static ScheduledExecutorService NewExecutorService()
		{
			return ThreadUtils.NewFixedThreadScheduledPool(2, "ChildReaper");
		}

		private void DoWork()
		{
			foreach (string path in paths)
			{
				try
				{
					IList<string> children = client.GetChildren().ForPath(path);
					foreach (string name in children)
					{
						string thisPath = ZKPaths.MakePath(path, name);
						Stat stat = client.CheckExists().ForPath(thisPath);
						if ((stat != null) && (stat.GetNumChildren() == 0))
						{
							reaper.AddPath(thisPath, mode);
						}
					}
				}
				catch (Exception e)
				{
					log.Error("Could not get children for path: " + path, e);
				}
			}
		}
	}
}
