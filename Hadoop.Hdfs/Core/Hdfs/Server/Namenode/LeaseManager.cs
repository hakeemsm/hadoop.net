using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>LeaseManager does the lease housekeeping for writing on files.</summary>
	/// <remarks>
	/// LeaseManager does the lease housekeeping for writing on files.
	/// This class also provides useful static methods for lease recovery.
	/// Lease Recovery Algorithm
	/// 1) Namenode retrieves lease information
	/// 2) For each file f in the lease, consider the last block b of f
	/// 2.1) Get the datanodes which contains b
	/// 2.2) Assign one of the datanodes as the primary datanode p
	/// 2.3) p obtains a new generation stamp from the namenode
	/// 2.4) p gets the block info from each datanode
	/// 2.5) p computes the minimum block length
	/// 2.6) p updates the datanodes, which have a valid generation stamp,
	/// with the new generation stamp and the minimum block length
	/// 2.7) p acknowledges the namenode the update results
	/// 2.8) Namenode updates the BlockInfo
	/// 2.9) Namenode removes f from the lease
	/// and removes the lease once all files have been removed
	/// 2.10) Namenode commit changes to edit log
	/// </remarks>
	public class LeaseManager
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.LeaseManager
			));

		private readonly FSNamesystem fsnamesystem;

		private long softLimit = HdfsConstants.LeaseSoftlimitPeriod;

		private long hardLimit = HdfsConstants.LeaseHardlimitPeriod;

		private readonly SortedDictionary<string, LeaseManager.Lease> leases = new SortedDictionary
			<string, LeaseManager.Lease>();

		private readonly NavigableSet<LeaseManager.Lease> sortedLeases = new TreeSet<LeaseManager.Lease
			>();

		private readonly SortedDictionary<string, LeaseManager.Lease> sortedLeasesByPath = 
			new SortedDictionary<string, LeaseManager.Lease>();

		private Daemon lmthread;

		private volatile bool shouldRunMonitor;

		internal LeaseManager(FSNamesystem fsnamesystem)
		{
			//
			// Used for handling lock-leases
			// Mapping: leaseHolder -> Lease
			//
			// Set of: Lease
			// 
			// Map path names to leases. It is protected by the sortedLeases lock.
			// The map stores pathnames in lexicographical order.
			//
			this.fsnamesystem = fsnamesystem;
		}

		internal virtual LeaseManager.Lease GetLease(string holder)
		{
			return leases[holder];
		}

		[VisibleForTesting]
		internal virtual int GetNumSortedLeases()
		{
			return sortedLeases.Count;
		}

		/// <summary>
		/// This method iterates through all the leases and counts the number of blocks
		/// which are not COMPLETE.
		/// </summary>
		/// <remarks>
		/// This method iterates through all the leases and counts the number of blocks
		/// which are not COMPLETE. The FSNamesystem read lock MUST be held before
		/// calling this method.
		/// </remarks>
		/// <returns/>
		internal virtual long GetNumUnderConstructionBlocks()
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(this.fsnamesystem.HasReadLock(), "The FSNamesystem read lock wasn't"
					 + "acquired before counting under construction blocks");
				long numUCBlocks = 0;
				foreach (LeaseManager.Lease lease in sortedLeases)
				{
					foreach (string path in lease.GetPaths())
					{
						INodeFile cons;
						try
						{
							cons = this.fsnamesystem.GetFSDirectory().GetINode(path).AsFile();
							if (!cons.IsUnderConstruction())
							{
								Log.Warn("The file " + cons.GetFullPathName() + " is not under construction but has lease."
									);
								continue;
							}
						}
						catch (UnresolvedLinkException)
						{
							throw new Exception("Lease files should reside on this FS");
						}
						BlockInfoContiguous[] blocks = cons.GetBlocks();
						if (blocks == null)
						{
							continue;
						}
						foreach (BlockInfoContiguous b in blocks)
						{
							if (!b.IsComplete())
							{
								numUCBlocks++;
							}
						}
					}
				}
				Log.Info("Number of blocks under construction: " + numUCBlocks);
				return numUCBlocks;
			}
		}

		/// <returns>the lease containing src</returns>
		public virtual LeaseManager.Lease GetLeaseByPath(string src)
		{
			return sortedLeasesByPath[src];
		}

		/// <returns>the number of leases currently in the system</returns>
		public virtual int CountLease()
		{
			lock (this)
			{
				return sortedLeases.Count;
			}
		}

		/// <returns>the number of paths contained in all leases</returns>
		internal virtual int CountPath()
		{
			lock (this)
			{
				int count = 0;
				foreach (LeaseManager.Lease lease in sortedLeases)
				{
					count += lease.GetPaths().Count;
				}
				return count;
			}
		}

		/// <summary>Adds (or re-adds) the lease for the specified file.</summary>
		internal virtual LeaseManager.Lease AddLease(string holder, string src)
		{
			lock (this)
			{
				LeaseManager.Lease lease = GetLease(holder);
				if (lease == null)
				{
					lease = new LeaseManager.Lease(this, holder);
					leases[holder] = lease;
					sortedLeases.AddItem(lease);
				}
				else
				{
					RenewLease(lease);
				}
				sortedLeasesByPath[src] = lease;
				lease.paths.AddItem(src);
				return lease;
			}
		}

		/// <summary>Remove the specified lease and src.</summary>
		internal virtual void RemoveLease(LeaseManager.Lease lease, string src)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(sortedLeasesByPath, src);
				if (!lease.RemovePath(src))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(src + " not found in lease.paths (=" + lease.paths + ")");
					}
				}
				if (!lease.HasPath())
				{
					Sharpen.Collections.Remove(leases, lease.holder);
					if (!sortedLeases.Remove(lease))
					{
						Log.Error(lease + " not found in sortedLeases");
					}
				}
			}
		}

		/// <summary>Remove the lease for the specified holder and src</summary>
		internal virtual void RemoveLease(string holder, string src)
		{
			lock (this)
			{
				LeaseManager.Lease lease = GetLease(holder);
				if (lease != null)
				{
					RemoveLease(lease, src);
				}
				else
				{
					Log.Warn("Removing non-existent lease! holder=" + holder + " src=" + src);
				}
			}
		}

		internal virtual void RemoveAllLeases()
		{
			lock (this)
			{
				sortedLeases.Clear();
				sortedLeasesByPath.Clear();
				leases.Clear();
			}
		}

		/// <summary>Reassign lease for file src to the new holder.</summary>
		internal virtual LeaseManager.Lease ReassignLease(LeaseManager.Lease lease, string
			 src, string newHolder)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(newHolder != null, "new lease holder is null");
				if (lease != null)
				{
					RemoveLease(lease, src);
				}
				return AddLease(newHolder, src);
			}
		}

		/// <summary>Renew the lease(s) held by the given client</summary>
		internal virtual void RenewLease(string holder)
		{
			lock (this)
			{
				RenewLease(GetLease(holder));
			}
		}

		internal virtual void RenewLease(LeaseManager.Lease lease)
		{
			lock (this)
			{
				if (lease != null)
				{
					sortedLeases.Remove(lease);
					lease.Renew();
					sortedLeases.AddItem(lease);
				}
			}
		}

		/// <summary>Renew all of the currently open leases.</summary>
		internal virtual void RenewAllLeases()
		{
			lock (this)
			{
				foreach (LeaseManager.Lease l in leases.Values)
				{
					RenewLease(l);
				}
			}
		}

		/// <summary>A Lease governs all the locks held by a single client.</summary>
		/// <remarks>
		/// A Lease governs all the locks held by a single client.
		/// For each client there's a corresponding lease, whose
		/// timestamp is updated when the client periodically
		/// checks in.  If the client dies and allows its lease to
		/// expire, all the corresponding locks can be released.
		/// </remarks>
		internal class Lease : Comparable<LeaseManager.Lease>
		{
			private readonly string holder;

			private long lastUpdate;

			private readonly ICollection<string> paths = new TreeSet<string>();

			/// <summary>Only LeaseManager object can create a lease</summary>
			private Lease(LeaseManager _enclosing, string holder)
			{
				this._enclosing = _enclosing;
				this.holder = holder;
				this.Renew();
			}

			/// <summary>Only LeaseManager object can renew a lease</summary>
			private void Renew()
			{
				this.lastUpdate = Time.MonotonicNow();
			}

			/// <returns>true if the Hard Limit Timer has expired</returns>
			public virtual bool ExpiredHardLimit()
			{
				return Time.MonotonicNow() - this.lastUpdate > this._enclosing.hardLimit;
			}

			/// <returns>true if the Soft Limit Timer has expired</returns>
			public virtual bool ExpiredSoftLimit()
			{
				return Time.MonotonicNow() - this.lastUpdate > this._enclosing.softLimit;
			}

			/// <summary>Does this lease contain any path?</summary>
			internal virtual bool HasPath()
			{
				return !this.paths.IsEmpty();
			}

			internal virtual bool RemovePath(string src)
			{
				return this.paths.Remove(src);
			}

			public override string ToString()
			{
				return "[Lease.  Holder: " + this.holder + ", pendingcreates: " + this.paths.Count
					 + "]";
			}

			public virtual int CompareTo(LeaseManager.Lease o)
			{
				LeaseManager.Lease l1 = this;
				LeaseManager.Lease l2 = o;
				long lu1 = l1.lastUpdate;
				long lu2 = l2.lastUpdate;
				if (lu1 < lu2)
				{
					return -1;
				}
				else
				{
					if (lu1 > lu2)
					{
						return 1;
					}
					else
					{
						return string.CompareOrdinal(l1.holder, l2.holder);
					}
				}
			}

			public override bool Equals(object o)
			{
				if (!(o is LeaseManager.Lease))
				{
					return false;
				}
				LeaseManager.Lease obj = (LeaseManager.Lease)o;
				if (this.lastUpdate == obj.lastUpdate && this.holder.Equals(obj.holder))
				{
					return true;
				}
				return false;
			}

			public override int GetHashCode()
			{
				return this.holder.GetHashCode();
			}

			internal virtual ICollection<string> GetPaths()
			{
				return this.paths;
			}

			internal virtual string GetHolder()
			{
				return this.holder;
			}

			internal virtual void ReplacePath(string oldpath, string newpath)
			{
				this.paths.Remove(oldpath);
				this.paths.AddItem(newpath);
			}

			[VisibleForTesting]
			internal virtual long GetLastUpdate()
			{
				return this.lastUpdate;
			}

			private readonly LeaseManager _enclosing;
		}

		internal virtual void ChangeLease(string src, string dst)
		{
			lock (this)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug(GetType().Name + ".changelease: " + " src=" + src + ", dest=" + dst);
				}
				int len = src.Length;
				foreach (KeyValuePair<string, LeaseManager.Lease> entry in FindLeaseWithPrefixPath
					(src, sortedLeasesByPath))
				{
					string oldpath = entry.Key;
					LeaseManager.Lease lease = entry.Value;
					// replace stem of src with new destination
					string newpath = dst + Sharpen.Runtime.Substring(oldpath, len);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("changeLease: replacing " + oldpath + " with " + newpath);
					}
					lease.ReplacePath(oldpath, newpath);
					Sharpen.Collections.Remove(sortedLeasesByPath, oldpath);
					sortedLeasesByPath[newpath] = lease;
				}
			}
		}

		internal virtual void RemoveLeaseWithPrefixPath(string prefix)
		{
			lock (this)
			{
				foreach (KeyValuePair<string, LeaseManager.Lease> entry in FindLeaseWithPrefixPath
					(prefix, sortedLeasesByPath))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(typeof(LeaseManager).Name + ".removeLeaseWithPrefixPath: entry=" + entry
							);
					}
					RemoveLease(entry.Value, entry.Key);
				}
			}
		}

		private static IDictionary<string, LeaseManager.Lease> FindLeaseWithPrefixPath(string
			 prefix, SortedDictionary<string, LeaseManager.Lease> path2lease)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug(typeof(LeaseManager).Name + ".findLease: prefix=" + prefix);
			}
			IDictionary<string, LeaseManager.Lease> entries = new Dictionary<string, LeaseManager.Lease
				>();
			int srclen = prefix.Length;
			// prefix may ended with '/'
			if (prefix[srclen - 1] == Path.SeparatorChar)
			{
				srclen -= 1;
			}
			foreach (KeyValuePair<string, LeaseManager.Lease> entry in path2lease.TailMap(prefix
				))
			{
				string p = entry.Key;
				if (!p.StartsWith(prefix))
				{
					return entries;
				}
				if (p.Length == srclen || p[srclen] == Path.SeparatorChar)
				{
					entries[entry.Key] = entry.Value;
				}
			}
			return entries;
		}

		public virtual void SetLeasePeriod(long softLimit, long hardLimit)
		{
			this.softLimit = softLimit;
			this.hardLimit = hardLimit;
		}

		/// <summary>
		/// Monitor checks for leases that have expired,
		/// and disposes of them.
		/// </summary>
		internal class Monitor : Runnable
		{
			internal readonly string name = this.GetType().Name;

			/// <summary>Check leases periodically.</summary>
			public virtual void Run()
			{
				for (; this._enclosing.shouldRunMonitor && this._enclosing.fsnamesystem.IsRunning
					(); )
				{
					bool needSync = false;
					try
					{
						this._enclosing.fsnamesystem.WriteLockInterruptibly();
						try
						{
							if (!this._enclosing.fsnamesystem.IsInSafeMode())
							{
								needSync = this._enclosing.CheckLeases();
							}
						}
						finally
						{
							this._enclosing.fsnamesystem.WriteUnlock();
							// lease reassignments should to be sync'ed.
							if (needSync)
							{
								this._enclosing.fsnamesystem.GetEditLog().LogSync();
							}
						}
						Sharpen.Thread.Sleep(HdfsServerConstants.NamenodeLeaseRecheckInterval);
					}
					catch (Exception ie)
					{
						if (LeaseManager.Log.IsDebugEnabled())
						{
							LeaseManager.Log.Debug(this.name + " is interrupted", ie);
						}
					}
				}
			}

			internal Monitor(LeaseManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly LeaseManager _enclosing;
		}

		/// <summary>Get the list of inodes corresponding to valid leases.</summary>
		/// <returns>list of inodes</returns>
		internal virtual IDictionary<string, INodeFile> GetINodesUnderConstruction()
		{
			IDictionary<string, INodeFile> inodes = new SortedDictionary<string, INodeFile>();
			foreach (string p in sortedLeasesByPath.Keys)
			{
				// verify that path exists in namespace
				try
				{
					INodeFile node = INodeFile.ValueOf(fsnamesystem.dir.GetINode(p), p);
					if (node.IsUnderConstruction())
					{
						inodes[p] = node;
					}
					else
					{
						Log.Warn("Ignore the lease of file " + p + " for checkpoint since the file is not under construction"
							);
					}
				}
				catch (IOException ioe)
				{
					Log.Error(ioe);
				}
			}
			return inodes;
		}

		/// <summary>Check the leases beginning from the oldest.</summary>
		/// <returns>true is sync is needed.</returns>
		[VisibleForTesting]
		internal virtual bool CheckLeases()
		{
			lock (this)
			{
				bool needSync = false;
				System.Diagnostics.Debug.Assert(fsnamesystem.HasWriteLock());
				LeaseManager.Lease leaseToCheck = null;
				try
				{
					leaseToCheck = sortedLeases.First();
				}
				catch (NoSuchElementException)
				{
				}
				while (leaseToCheck != null)
				{
					if (!leaseToCheck.ExpiredHardLimit())
					{
						break;
					}
					Log.Info(leaseToCheck + " has expired hard limit");
					IList<string> removing = new AList<string>();
					// need to create a copy of the oldest lease paths, because 
					// internalReleaseLease() removes paths corresponding to empty files,
					// i.e. it needs to modify the collection being iterated over
					// causing ConcurrentModificationException
					string[] leasePaths = new string[leaseToCheck.GetPaths().Count];
					Sharpen.Collections.ToArray(leaseToCheck.GetPaths(), leasePaths);
					foreach (string p in leasePaths)
					{
						try
						{
							INodesInPath iip = fsnamesystem.GetFSDirectory().GetINodesInPath(p, true);
							bool completed = fsnamesystem.InternalReleaseLease(leaseToCheck, p, iip, HdfsServerConstants
								.NamenodeLeaseHolder);
							if (Log.IsDebugEnabled())
							{
								if (completed)
								{
									Log.Debug("Lease recovery for " + p + " is complete. File closed.");
								}
								else
								{
									Log.Debug("Started block recovery " + p + " lease " + leaseToCheck);
								}
							}
							// If a lease recovery happened, we need to sync later.
							if (!needSync && !completed)
							{
								needSync = true;
							}
						}
						catch (IOException e)
						{
							Log.Error("Cannot release the path " + p + " in the lease " + leaseToCheck, e);
							removing.AddItem(p);
						}
					}
					foreach (string p_1 in removing)
					{
						RemoveLease(leaseToCheck, p_1);
					}
					leaseToCheck = sortedLeases.Higher(leaseToCheck);
				}
				try
				{
					if (leaseToCheck != sortedLeases.First())
					{
						Log.Warn("Unable to release hard-limit expired lease: " + sortedLeases.First());
					}
				}
				catch (NoSuchElementException)
				{
				}
				return needSync;
			}
		}

		public override string ToString()
		{
			lock (this)
			{
				return GetType().Name + "= {" + "\n leases=" + leases + "\n sortedLeases=" + sortedLeases
					 + "\n sortedLeasesByPath=" + sortedLeasesByPath + "\n}";
			}
		}

		internal virtual void StartMonitor()
		{
			Preconditions.CheckState(lmthread == null, "Lease Monitor already running");
			shouldRunMonitor = true;
			lmthread = new Daemon(new LeaseManager.Monitor(this));
			lmthread.Start();
		}

		internal virtual void StopMonitor()
		{
			if (lmthread != null)
			{
				shouldRunMonitor = false;
				try
				{
					lmthread.Interrupt();
					lmthread.Join(3000);
				}
				catch (Exception ie)
				{
					Log.Warn("Encountered exception ", ie);
				}
				lmthread = null;
			}
		}

		/// <summary>
		/// Trigger the currently-running Lease monitor to re-check
		/// its leases immediately.
		/// </summary>
		/// <remarks>
		/// Trigger the currently-running Lease monitor to re-check
		/// its leases immediately. This is for use by unit tests.
		/// </remarks>
		[VisibleForTesting]
		internal virtual void TriggerMonitorCheckNow()
		{
			Preconditions.CheckState(lmthread != null, "Lease monitor is not running");
			lmthread.Interrupt();
		}
	}
}
