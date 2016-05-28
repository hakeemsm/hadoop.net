using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Manages the BPOfferService objects for the data node.</summary>
	/// <remarks>
	/// Manages the BPOfferService objects for the data node.
	/// Creation, removal, starting, stopping, shutdown on BPOfferService
	/// objects must be done via APIs in this class.
	/// </remarks>
	internal class BlockPoolManager
	{
		private static readonly Log Log = DataNode.Log;

		private readonly IDictionary<string, BPOfferService> bpByNameserviceId = Maps.NewHashMap
			();

		private readonly IDictionary<string, BPOfferService> bpByBlockPoolId = Maps.NewHashMap
			();

		private readonly IList<BPOfferService> offerServices = Lists.NewArrayList();

		private readonly DataNode dn;

		private readonly object refreshNamenodesLock = new object();

		internal BlockPoolManager(DataNode dn)
		{
			//This lock is used only to ensure exclusion of refreshNamenodes
			this.dn = dn;
		}

		internal virtual void AddBlockPool(BPOfferService bpos)
		{
			lock (this)
			{
				Preconditions.CheckArgument(offerServices.Contains(bpos), "Unknown BPOS: %s", bpos
					);
				if (bpos.GetBlockPoolId() == null)
				{
					throw new ArgumentException("Null blockpool id");
				}
				bpByBlockPoolId[bpos.GetBlockPoolId()] = bpos;
			}
		}

		/// <summary>Returns the array of BPOfferService objects.</summary>
		/// <remarks>
		/// Returns the array of BPOfferService objects.
		/// Caution: The BPOfferService returned could be shutdown any time.
		/// </remarks>
		internal virtual BPOfferService[] GetAllNamenodeThreads()
		{
			lock (this)
			{
				BPOfferService[] bposArray = new BPOfferService[offerServices.Count];
				return Sharpen.Collections.ToArray(offerServices, bposArray);
			}
		}

		internal virtual BPOfferService Get(string bpid)
		{
			lock (this)
			{
				return bpByBlockPoolId[bpid];
			}
		}

		internal virtual void Remove(BPOfferService t)
		{
			lock (this)
			{
				offerServices.Remove(t);
				if (t.HasBlockPoolId())
				{
					// It's possible that the block pool never successfully registered
					// with any NN, so it was never added it to this map
					Sharpen.Collections.Remove(bpByBlockPoolId, t.GetBlockPoolId());
				}
				bool removed = false;
				for (IEnumerator<BPOfferService> it = bpByNameserviceId.Values.GetEnumerator(); it
					.HasNext() && !removed; )
				{
					BPOfferService bpos = it.Next();
					if (bpos == t)
					{
						it.Remove();
						Log.Info("Removed " + bpos);
						removed = true;
					}
				}
				if (!removed)
				{
					Log.Warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal virtual void ShutDownAll(BPOfferService[] bposArray)
		{
			if (bposArray != null)
			{
				foreach (BPOfferService bpos in bposArray)
				{
					bpos.Stop();
				}
				//interrupts the threads
				//now join
				foreach (BPOfferService bpos_1 in bposArray)
				{
					bpos_1.Join();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void StartAll()
		{
			lock (this)
			{
				try
				{
					UserGroupInformation.GetLoginUser().DoAs(new _PrivilegedExceptionAction_128(this)
						);
				}
				catch (Exception ex)
				{
					IOException ioe = new IOException();
					Sharpen.Extensions.InitCause(ioe, ex.InnerException);
					throw ioe;
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_128 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_128(BlockPoolManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				foreach (BPOfferService bpos in this._enclosing.offerServices)
				{
					bpos.Start();
				}
				return null;
			}

			private readonly BlockPoolManager _enclosing;
		}

		internal virtual void JoinAll()
		{
			foreach (BPOfferService bpos in this.GetAllNamenodeThreads())
			{
				bpos.Join();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RefreshNamenodes(Configuration conf)
		{
			Log.Info("Refresh request received for nameservices: " + conf.Get(DFSConfigKeys.DfsNameservices
				));
			IDictionary<string, IDictionary<string, IPEndPoint>> newAddressMap = DFSUtil.GetNNServiceRpcAddressesForCluster
				(conf);
			lock (refreshNamenodesLock)
			{
				DoRefreshNamenodes(newAddressMap);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoRefreshNamenodes(IDictionary<string, IDictionary<string, IPEndPoint
			>> addrMap)
		{
			System.Diagnostics.Debug.Assert(Sharpen.Thread.HoldsLock(refreshNamenodesLock));
			ICollection<string> toRefresh = Sets.NewLinkedHashSet();
			ICollection<string> toAdd = Sets.NewLinkedHashSet();
			ICollection<string> toRemove;
			lock (this)
			{
				// Step 1. For each of the new nameservices, figure out whether
				// it's an update of the set of NNs for an existing NS,
				// or an entirely new nameservice.
				foreach (string nameserviceId in addrMap.Keys)
				{
					if (bpByNameserviceId.Contains(nameserviceId))
					{
						toRefresh.AddItem(nameserviceId);
					}
					else
					{
						toAdd.AddItem(nameserviceId);
					}
				}
				// Step 2. Any nameservices we currently have but are no longer present
				// need to be removed.
				toRemove = Sets.NewHashSet(Sets.Difference(bpByNameserviceId.Keys, addrMap.Keys));
				System.Diagnostics.Debug.Assert(toRefresh.Count + toAdd.Count == addrMap.Count, "toAdd: "
					 + Joiner.On(",").UseForNull("<default>").Join(toAdd) + "  toRemove: " + Joiner.
					On(",").UseForNull("<default>").Join(toRemove) + "  toRefresh: " + Joiner.On(","
					).UseForNull("<default>").Join(toRefresh));
				// Step 3. Start new nameservices
				if (!toAdd.IsEmpty())
				{
					Log.Info("Starting BPOfferServices for nameservices: " + Joiner.On(",").UseForNull
						("<default>").Join(toAdd));
					foreach (string nsToAdd in toAdd)
					{
						AList<IPEndPoint> addrs = Lists.NewArrayList(addrMap[nsToAdd].Values);
						BPOfferService bpos = CreateBPOS(addrs);
						bpByNameserviceId[nsToAdd] = bpos;
						offerServices.AddItem(bpos);
					}
				}
				StartAll();
			}
			// Step 4. Shut down old nameservices. This happens outside
			// of the synchronized(this) lock since they need to call
			// back to .remove() from another thread
			if (!toRemove.IsEmpty())
			{
				Log.Info("Stopping BPOfferServices for nameservices: " + Joiner.On(",").UseForNull
					("<default>").Join(toRemove));
				foreach (string nsToRemove in toRemove)
				{
					BPOfferService bpos = bpByNameserviceId[nsToRemove];
					bpos.Stop();
					bpos.Join();
				}
			}
			// they will call remove on their own
			// Step 5. Update nameservices whose NN list has changed
			if (!toRefresh.IsEmpty())
			{
				Log.Info("Refreshing list of NNs for nameservices: " + Joiner.On(",").UseForNull(
					"<default>").Join(toRefresh));
				foreach (string nsToRefresh in toRefresh)
				{
					BPOfferService bpos = bpByNameserviceId[nsToRefresh];
					AList<IPEndPoint> addrs = Lists.NewArrayList(addrMap[nsToRefresh].Values);
					bpos.RefreshNNList(addrs);
				}
			}
		}

		/// <summary>Extracted out for test purposes.</summary>
		protected internal virtual BPOfferService CreateBPOS(IList<IPEndPoint> nnAddrs)
		{
			return new BPOfferService(nnAddrs, dn);
		}
	}
}
