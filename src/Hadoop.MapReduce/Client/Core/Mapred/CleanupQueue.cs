using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	internal class CleanupQueue
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.CleanupQueue
			));

		private static CleanupQueue.PathCleanupThread cleanupThread;

		/// <summary>Create a singleton path-clean-up queue.</summary>
		/// <remarks>
		/// Create a singleton path-clean-up queue. It can be used to delete
		/// paths(directories/files) in a separate thread. This constructor creates a
		/// clean-up thread and also starts it as a daemon. Callers can instantiate one
		/// CleanupQueue per JVM and can use it for deleting paths. Use
		/// <see cref="AddToQueue(PathDeletionContext[])"/>
		/// to add paths for
		/// deletion.
		/// </remarks>
		public CleanupQueue()
		{
			lock (typeof(CleanupQueue.PathCleanupThread))
			{
				if (cleanupThread == null)
				{
					cleanupThread = new CleanupQueue.PathCleanupThread();
				}
			}
		}

		/// <summary>Contains info related to the path of the file/dir to be deleted</summary>
		internal class PathDeletionContext
		{
			internal string fullPath;

			internal FileSystem fs;

			public PathDeletionContext(FileSystem fs, string fullPath)
			{
				// full path of file or dir
				this.fs = fs;
				this.fullPath = fullPath;
			}

			protected internal virtual string GetPathForCleanup()
			{
				return fullPath;
			}

			/// <summary>Makes the path(and its subdirectories recursively) fully deletable</summary>
			/// <exception cref="System.IO.IOException"/>
			protected internal virtual void EnablePathForCleanup()
			{
			}
			// Do nothing by default.
			// Subclasses can override to provide enabling for deletion.
		}

		/// <summary>Adds the paths to the queue of paths to be deleted by cleanupThread.</summary>
		internal virtual void AddToQueue(params CleanupQueue.PathDeletionContext[] contexts
			)
		{
			cleanupThread.AddToQueue(contexts);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static bool DeletePath(CleanupQueue.PathDeletionContext context
			)
		{
			context.EnablePathForCleanup();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Trying to delete " + context.fullPath);
			}
			if (context.fs.Exists(new Path(context.fullPath)))
			{
				return context.fs.Delete(new Path(context.fullPath), true);
			}
			return true;
		}

		// currently used by tests only
		protected internal virtual bool IsQueueEmpty()
		{
			return (cleanupThread.queue.Count == 0);
		}

		private class PathCleanupThread : Sharpen.Thread
		{
			private LinkedBlockingQueue<CleanupQueue.PathDeletionContext> queue = new LinkedBlockingQueue
				<CleanupQueue.PathDeletionContext>();

			public PathCleanupThread()
			{
				// cleanup queue which deletes files/directories of the paths queued up.
				SetName("Directory/File cleanup thread");
				SetDaemon(true);
				Start();
			}

			internal virtual void AddToQueue(CleanupQueue.PathDeletionContext[] contexts)
			{
				foreach (CleanupQueue.PathDeletionContext context in contexts)
				{
					try
					{
						queue.Put(context);
					}
					catch (Exception)
					{
					}
				}
			}

			public override void Run()
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug(GetName() + " started.");
				}
				CleanupQueue.PathDeletionContext context = null;
				while (true)
				{
					try
					{
						context = queue.Take();
						// delete the path.
						if (!DeletePath(context))
						{
							Log.Warn("CleanupThread:Unable to delete path " + context.fullPath);
						}
						else
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("DELETED " + context.fullPath);
							}
						}
					}
					catch (Exception)
					{
						Log.Warn("Interrupted deletion of " + context.fullPath);
						return;
					}
					catch (Exception e)
					{
						Log.Warn("Error deleting path " + context.fullPath + ": " + e);
					}
				}
			}
		}
	}
}
