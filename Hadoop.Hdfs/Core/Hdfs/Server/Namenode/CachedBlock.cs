using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Represents a cached block.</summary>
	public sealed class CachedBlock : IntrusiveCollection.Element, LightWeightGSet.LinkedElement
	{
		private static readonly object[] EmptyArray = new object[0];

		/// <summary>Block id.</summary>
		private readonly long blockId;

		/// <summary>Used to implement #{LightWeightGSet.LinkedElement}</summary>
		private LightWeightGSet.LinkedElement nextElement;

		/// <summary>
		/// Bit 15: Mark
		/// Bit 0-14: cache replication factor.
		/// </summary>
		private short replicationAndMark;

		/// <summary>Used to implement the CachedBlocksList.</summary>
		/// <remarks>
		/// Used to implement the CachedBlocksList.
		/// Since this CachedBlock can be in multiple CachedBlocksList objects,
		/// we need to be able to store multiple 'prev' and 'next' pointers.
		/// The triplets array does this.
		/// Each triplet contains a CachedBlockList object followed by a
		/// prev pointer, followed by a next pointer.
		/// </remarks>
		private object[] triplets;

		public CachedBlock(long blockId, short replication, bool mark)
		{
			this.blockId = blockId;
			this.triplets = EmptyArray;
			SetReplicationAndMark(replication, mark);
		}

		public long GetBlockId()
		{
			return blockId;
		}

		public override int GetHashCode()
		{
			return (int)(blockId ^ ((long)(((ulong)blockId) >> 32)));
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (o == this)
			{
				return true;
			}
			if (o.GetType() != this.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Server.Namenode.CachedBlock other = (Org.Apache.Hadoop.Hdfs.Server.Namenode.CachedBlock
				)o;
			return other.blockId == blockId;
		}

		public void SetReplicationAndMark(short replication, bool mark)
		{
			System.Diagnostics.Debug.Assert(replication >= 0);
			replicationAndMark = (short)((replication << 1) | (mark ? unchecked((int)(0x1)) : 
				unchecked((int)(0x0))));
		}

		public bool GetMark()
		{
			return ((replicationAndMark & unchecked((int)(0x1))) != 0);
		}

		public short GetReplication()
		{
			return (short)((short)(((ushort)replicationAndMark) >> 1));
		}

		/// <summary>Return true if this CachedBlock is present on the given list.</summary>
		public bool IsPresent(DatanodeDescriptor.CachedBlocksList cachedBlocksList)
		{
			for (int i = 0; i < triplets.Length; i += 3)
			{
				DatanodeDescriptor.CachedBlocksList list = (DatanodeDescriptor.CachedBlocksList)triplets
					[i];
				if (list == cachedBlocksList)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Get a list of the datanodes which this block is cached,
		/// planned to be cached, or planned to be uncached on.
		/// </summary>
		/// <param name="type">
		/// If null, this parameter is ignored.
		/// If it is non-null, we match only datanodes which
		/// have it on this list.
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeDescriptor.CachedBlocksList.Type
		/// 	"/>
		/// for a description of all the lists.
		/// </param>
		/// <returns>
		/// The list of datanodes.  Modifying this list does not
		/// alter the state of the CachedBlock.
		/// </returns>
		public IList<DatanodeDescriptor> GetDatanodes(DatanodeDescriptor.CachedBlocksList.Type
			 type)
		{
			IList<DatanodeDescriptor> nodes = new List<DatanodeDescriptor>();
			for (int i = 0; i < triplets.Length; i += 3)
			{
				DatanodeDescriptor.CachedBlocksList list = (DatanodeDescriptor.CachedBlocksList)triplets
					[i];
				if ((type == null) || (list.GetType() == type))
				{
					nodes.AddItem(list.GetDatanode());
				}
			}
			return nodes;
		}

		public void InsertInternal<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
			 prev, IntrusiveCollection.Element next)
			where _T0 : IntrusiveCollection.Element
		{
			for (int i = 0; i < triplets.Length; i += 3)
			{
				if (triplets[i] == list)
				{
					throw new RuntimeException("Trying to re-insert an element that " + "is already in the list."
						);
				}
			}
			object[] newTriplets = Arrays.CopyOf(triplets, triplets.Length + 3);
			newTriplets[triplets.Length] = list;
			newTriplets[triplets.Length + 1] = prev;
			newTriplets[triplets.Length + 2] = next;
			triplets = newTriplets;
		}

		public void SetPrev<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
			 prev)
			where _T0 : IntrusiveCollection.Element
		{
			for (int i = 0; i < triplets.Length; i += 3)
			{
				if (triplets[i] == list)
				{
					triplets[i + 1] = prev;
					return;
				}
			}
			throw new RuntimeException("Called setPrev on an element that wasn't " + "in the list."
				);
		}

		public void SetNext<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
			 next)
			where _T0 : IntrusiveCollection.Element
		{
			for (int i = 0; i < triplets.Length; i += 3)
			{
				if (triplets[i] == list)
				{
					triplets[i + 2] = next;
					return;
				}
			}
			throw new RuntimeException("Called setNext on an element that wasn't " + "in the list."
				);
		}

		public void RemoveInternal<_T0>(IntrusiveCollection<_T0> list)
			where _T0 : IntrusiveCollection.Element
		{
			for (int i = 0; i < triplets.Length; i += 3)
			{
				if (triplets[i] == list)
				{
					object[] newTriplets = new object[triplets.Length - 3];
					System.Array.Copy(triplets, 0, newTriplets, 0, i);
					System.Array.Copy(triplets, i + 3, newTriplets, i, triplets.Length - (i + 3));
					triplets = newTriplets;
					return;
				}
			}
			throw new RuntimeException("Called remove on an element that wasn't " + "in the list."
				);
		}

		public IntrusiveCollection.Element GetPrev<_T0>(IntrusiveCollection<_T0> list)
			where _T0 : IntrusiveCollection.Element
		{
			for (int i = 0; i < triplets.Length; i += 3)
			{
				if (triplets[i] == list)
				{
					return (IntrusiveCollection.Element)triplets[i + 1];
				}
			}
			throw new RuntimeException("Called getPrev on an element that wasn't " + "in the list."
				);
		}

		public IntrusiveCollection.Element GetNext<_T0>(IntrusiveCollection<_T0> list)
			where _T0 : IntrusiveCollection.Element
		{
			for (int i = 0; i < triplets.Length; i += 3)
			{
				if (triplets[i] == list)
				{
					return (IntrusiveCollection.Element)triplets[i + 2];
				}
			}
			throw new RuntimeException("Called getNext on an element that wasn't " + "in the list."
				);
		}

		public bool IsInList<_T0>(IntrusiveCollection<_T0> list)
			where _T0 : IntrusiveCollection.Element
		{
			for (int i = 0; i < triplets.Length; i += 3)
			{
				if (triplets[i] == list)
				{
					return true;
				}
			}
			return false;
		}

		public override string ToString()
		{
			return new StringBuilder().Append("{").Append("blockId=").Append(blockId).Append(
				", ").Append("replication=").Append(GetReplication()).Append(", ").Append("mark="
				).Append(GetMark()).Append("}").ToString();
		}

		public void SetNext(LightWeightGSet.LinkedElement next)
		{
			// LightWeightGSet.LinkedElement 
			this.nextElement = next;
		}

		public LightWeightGSet.LinkedElement GetNext()
		{
			// LightWeightGSet.LinkedElement 
			return nextElement;
		}
	}
}
