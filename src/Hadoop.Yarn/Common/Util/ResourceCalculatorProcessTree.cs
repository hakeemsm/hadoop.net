using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// Interface class to obtain process resource usage
	/// NOTE: This class should not be used by external users, but only by external
	/// developers to extend and include their own process-tree implementation,
	/// especially for platforms other than Linux and Windows.
	/// </summary>
	public abstract class ResourceCalculatorProcessTree : Configured
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Util.ResourceCalculatorProcessTree
			));

		public const int Unavailable = -1;

		/// <summary>Create process-tree instance with specified root process.</summary>
		/// <remarks>
		/// Create process-tree instance with specified root process.
		/// Subclass must override this.
		/// </remarks>
		/// <param name="root">process-tree root-process</param>
		public ResourceCalculatorProcessTree(string root)
		{
		}

		/// <summary>Update the process-tree with latest state.</summary>
		/// <remarks>
		/// Update the process-tree with latest state.
		/// Each call to this function should increment the age of the running
		/// processes that already exist in the process tree. Age is used other API's
		/// of the interface.
		/// </remarks>
		public abstract void UpdateProcessTree();

		/// <summary>Get a dump of the process-tree.</summary>
		/// <returns>
		/// a string concatenating the dump of information of all the processes
		/// in the process-tree
		/// </returns>
		public abstract string GetProcessTreeDump();

		/// <summary>
		/// Get the virtual memory used by all the processes in the
		/// process-tree.
		/// </summary>
		/// <returns>
		/// virtual memory used by the process-tree in bytes,
		/// <see cref="Unavailable"/>
		/// if it cannot be calculated.
		/// </returns>
		public virtual long GetVirtualMemorySize()
		{
			return GetVirtualMemorySize(0);
		}

		/// <summary>
		/// Get the virtual memory used by all the processes in the
		/// process-tree.
		/// </summary>
		/// <returns>
		/// virtual memory used by the process-tree in bytes,
		/// <see cref="Unavailable"/>
		/// if it cannot be calculated.
		/// </returns>
		[Obsolete]
		public virtual long GetCumulativeVmem()
		{
			return GetCumulativeVmem(0);
		}

		/// <summary>
		/// Get the resident set size (rss) memory used by all the processes
		/// in the process-tree.
		/// </summary>
		/// <returns>
		/// rss memory used by the process-tree in bytes,
		/// <see cref="Unavailable"/>
		/// if it cannot be calculated.
		/// </returns>
		public virtual long GetRssMemorySize()
		{
			return GetRssMemorySize(0);
		}

		/// <summary>
		/// Get the resident set size (rss) memory used by all the processes
		/// in the process-tree.
		/// </summary>
		/// <returns>
		/// rss memory used by the process-tree in bytes,
		/// <see cref="Unavailable"/>
		/// if it cannot be calculated.
		/// </returns>
		[Obsolete]
		public virtual long GetCumulativeRssmem()
		{
			return GetCumulativeRssmem(0);
		}

		/// <summary>
		/// Get the virtual memory used by all the processes in the
		/// process-tree that are older than the passed in age.
		/// </summary>
		/// <param name="olderThanAge">
		/// processes above this age are included in the
		/// memory addition
		/// </param>
		/// <returns>
		/// virtual memory used by the process-tree in bytes for
		/// processes older than the specified age,
		/// <see cref="Unavailable"/>
		/// if it
		/// cannot be calculated.
		/// </returns>
		public virtual long GetVirtualMemorySize(int olderThanAge)
		{
			return Unavailable;
		}

		/// <summary>
		/// Get the virtual memory used by all the processes in the
		/// process-tree that are older than the passed in age.
		/// </summary>
		/// <param name="olderThanAge">
		/// processes above this age are included in the
		/// memory addition
		/// </param>
		/// <returns>
		/// virtual memory used by the process-tree in bytes for
		/// processes older than the specified age,
		/// <see cref="Unavailable"/>
		/// if it
		/// cannot be calculated.
		/// </returns>
		[Obsolete]
		public virtual long GetCumulativeVmem(int olderThanAge)
		{
			return Unavailable;
		}

		/// <summary>
		/// Get the resident set size (rss) memory used by all the processes
		/// in the process-tree that are older than the passed in age.
		/// </summary>
		/// <param name="olderThanAge">
		/// processes above this age are included in the
		/// memory addition
		/// </param>
		/// <returns>
		/// rss memory used by the process-tree in bytes for
		/// processes older than specified age,
		/// <see cref="Unavailable"/>
		/// if it cannot be
		/// calculated.
		/// </returns>
		public virtual long GetRssMemorySize(int olderThanAge)
		{
			return Unavailable;
		}

		/// <summary>
		/// Get the resident set size (rss) memory used by all the processes
		/// in the process-tree that are older than the passed in age.
		/// </summary>
		/// <param name="olderThanAge">
		/// processes above this age are included in the
		/// memory addition
		/// </param>
		/// <returns>
		/// rss memory used by the process-tree in bytes for
		/// processes older than specified age,
		/// <see cref="Unavailable"/>
		/// if it cannot be
		/// calculated.
		/// </returns>
		[Obsolete]
		public virtual long GetCumulativeRssmem(int olderThanAge)
		{
			return Unavailable;
		}

		/// <summary>
		/// Get the CPU time in millisecond used by all the processes in the
		/// process-tree since the process-tree was created
		/// </summary>
		/// <returns>
		/// cumulative CPU time in millisecond since the process-tree
		/// created,
		/// <see cref="Unavailable"/>
		/// if it cannot be calculated.
		/// </returns>
		public virtual long GetCumulativeCpuTime()
		{
			return Unavailable;
		}

		/// <summary>
		/// Get the CPU usage by all the processes in the process-tree based on
		/// average between samples as a ratio of overall CPU cycles similar to top.
		/// </summary>
		/// <remarks>
		/// Get the CPU usage by all the processes in the process-tree based on
		/// average between samples as a ratio of overall CPU cycles similar to top.
		/// Thus, if 2 out of 4 cores are used this should return 200.0.
		/// </remarks>
		/// <returns>
		/// percentage CPU usage since the process-tree was created,
		/// <see cref="Unavailable"/>
		/// if it cannot be calculated.
		/// </returns>
		public virtual float GetCpuUsagePercent()
		{
			return Unavailable;
		}

		/// <summary>Verify that the tree process id is same as its process group id.</summary>
		/// <returns>true if the process id matches else return false.</returns>
		public abstract bool CheckPidPgrpidForMatch();

		/// <summary>
		/// Create the ResourceCalculatorProcessTree rooted to specified process
		/// from the class name and configure it.
		/// </summary>
		/// <remarks>
		/// Create the ResourceCalculatorProcessTree rooted to specified process
		/// from the class name and configure it. If class name is null, this method
		/// will try and return a process tree plugin available for this system.
		/// </remarks>
		/// <param name="pid">process pid of the root of the process tree</param>
		/// <param name="clazz">class-name</param>
		/// <param name="conf">configure the plugin with this.</param>
		/// <returns>
		/// ResourceCalculatorProcessTree or null if ResourceCalculatorPluginTree
		/// is not available for this system.
		/// </returns>
		public static Org.Apache.Hadoop.Yarn.Util.ResourceCalculatorProcessTree GetResourceCalculatorProcessTree
			(string pid, Type clazz, Configuration conf)
		{
			if (clazz != null)
			{
				try
				{
					Constructor<Org.Apache.Hadoop.Yarn.Util.ResourceCalculatorProcessTree> c = clazz.
						GetConstructor(typeof(string));
					Org.Apache.Hadoop.Yarn.Util.ResourceCalculatorProcessTree rctree = c.NewInstance(
						pid);
					rctree.SetConf(conf);
					return rctree;
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}
			// No class given, try a os specific class
			if (ProcfsBasedProcessTree.IsAvailable())
			{
				return new ProcfsBasedProcessTree(pid);
			}
			if (WindowsBasedProcessTree.IsAvailable())
			{
				return new WindowsBasedProcessTree(pid);
			}
			// Not supported on this system.
			return null;
		}
	}
}
