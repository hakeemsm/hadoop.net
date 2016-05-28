using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Manages the list of encryption zones in the filesystem.</summary>
	/// <remarks>
	/// Manages the list of encryption zones in the filesystem.
	/// <p/>
	/// The EncryptionZoneManager has its own lock, but relies on the FSDirectory
	/// lock being held for many operations. The FSDirectory lock should not be
	/// taken if the manager lock is already held.
	/// </remarks>
	public class EncryptionZoneManager
	{
		public static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.EncryptionZoneManager
			));

		/// <summary>EncryptionZoneInt is the internal representation of an encryption zone.</summary>
		/// <remarks>
		/// EncryptionZoneInt is the internal representation of an encryption zone. The
		/// external representation of an EZ is embodied in an EncryptionZone and
		/// contains the EZ's pathname.
		/// </remarks>
		private class EncryptionZoneInt
		{
			private readonly long inodeId;

			private readonly CipherSuite suite;

			private readonly CryptoProtocolVersion version;

			private readonly string keyName;

			internal EncryptionZoneInt(long inodeId, CipherSuite suite, CryptoProtocolVersion
				 version, string keyName)
			{
				Preconditions.CheckArgument(suite != CipherSuite.Unknown);
				Preconditions.CheckArgument(version != CryptoProtocolVersion.Unknown);
				this.inodeId = inodeId;
				this.suite = suite;
				this.version = version;
				this.keyName = keyName;
			}

			internal virtual long GetINodeId()
			{
				return inodeId;
			}

			internal virtual CipherSuite GetSuite()
			{
				return suite;
			}

			internal virtual CryptoProtocolVersion GetVersion()
			{
				return version;
			}

			internal virtual string GetKeyName()
			{
				return keyName;
			}
		}

		private readonly SortedDictionary<long, EncryptionZoneManager.EncryptionZoneInt> 
			encryptionZones;

		private readonly FSDirectory dir;

		private readonly int maxListEncryptionZonesResponses;

		/// <summary>Construct a new EncryptionZoneManager.</summary>
		/// <param name="dir">Enclosing FSDirectory</param>
		public EncryptionZoneManager(FSDirectory dir, Configuration conf)
		{
			this.dir = dir;
			encryptionZones = new SortedDictionary<long, EncryptionZoneManager.EncryptionZoneInt
				>();
			maxListEncryptionZonesResponses = conf.GetInt(DFSConfigKeys.DfsNamenodeListEncryptionZonesNumResponses
				, DFSConfigKeys.DfsNamenodeListEncryptionZonesNumResponsesDefault);
			Preconditions.CheckArgument(maxListEncryptionZonesResponses >= 0, DFSConfigKeys.DfsNamenodeListEncryptionZonesNumResponses
				 + " " + "must be a positive integer.");
		}

		/// <summary>Add a new encryption zone.</summary>
		/// <remarks>
		/// Add a new encryption zone.
		/// <p/>
		/// Called while holding the FSDirectory lock.
		/// </remarks>
		/// <param name="inodeId">of the encryption zone</param>
		/// <param name="keyName">encryption zone key name</param>
		internal virtual void AddEncryptionZone(long inodeId, CipherSuite suite, CryptoProtocolVersion
			 version, string keyName)
		{
			System.Diagnostics.Debug.Assert(dir.HasWriteLock());
			UnprotectedAddEncryptionZone(inodeId, suite, version, keyName);
		}

		/// <summary>Add a new encryption zone.</summary>
		/// <remarks>
		/// Add a new encryption zone.
		/// <p/>
		/// Does not assume that the FSDirectory lock is held.
		/// </remarks>
		/// <param name="inodeId">of the encryption zone</param>
		/// <param name="keyName">encryption zone key name</param>
		internal virtual void UnprotectedAddEncryptionZone(long inodeId, CipherSuite suite
			, CryptoProtocolVersion version, string keyName)
		{
			EncryptionZoneManager.EncryptionZoneInt ez = new EncryptionZoneManager.EncryptionZoneInt
				(inodeId, suite, version, keyName);
			encryptionZones[inodeId] = ez;
		}

		/// <summary>Remove an encryption zone.</summary>
		/// <remarks>
		/// Remove an encryption zone.
		/// <p/>
		/// Called while holding the FSDirectory lock.
		/// </remarks>
		internal virtual void RemoveEncryptionZone(long inodeId)
		{
			System.Diagnostics.Debug.Assert(dir.HasWriteLock());
			Sharpen.Collections.Remove(encryptionZones, inodeId);
		}

		/// <summary>Returns true if an IIP is within an encryption zone.</summary>
		/// <remarks>
		/// Returns true if an IIP is within an encryption zone.
		/// <p/>
		/// Called while holding the FSDirectory lock.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		internal virtual bool IsInAnEZ(INodesInPath iip)
		{
			System.Diagnostics.Debug.Assert(dir.HasReadLock());
			return (GetEncryptionZoneForPath(iip) != null);
		}

		/// <summary>Returns the path of the EncryptionZoneInt.</summary>
		/// <remarks>
		/// Returns the path of the EncryptionZoneInt.
		/// <p/>
		/// Called while holding the FSDirectory lock.
		/// </remarks>
		private string GetFullPathName(EncryptionZoneManager.EncryptionZoneInt ezi)
		{
			System.Diagnostics.Debug.Assert(dir.HasReadLock());
			return dir.GetInode(ezi.GetINodeId()).GetFullPathName();
		}

		/// <summary>Get the key name for an encryption zone.</summary>
		/// <remarks>
		/// Get the key name for an encryption zone. Returns null if <tt>iip</tt> is
		/// not within an encryption zone.
		/// <p/>
		/// Called while holding the FSDirectory lock.
		/// </remarks>
		internal virtual string GetKeyName(INodesInPath iip)
		{
			System.Diagnostics.Debug.Assert(dir.HasReadLock());
			EncryptionZoneManager.EncryptionZoneInt ezi = GetEncryptionZoneForPath(iip);
			if (ezi == null)
			{
				return null;
			}
			return ezi.GetKeyName();
		}

		/// <summary>Looks up the EncryptionZoneInt for a path within an encryption zone.</summary>
		/// <remarks>
		/// Looks up the EncryptionZoneInt for a path within an encryption zone.
		/// Returns null if path is not within an EZ.
		/// <p/>
		/// Must be called while holding the manager lock.
		/// </remarks>
		private EncryptionZoneManager.EncryptionZoneInt GetEncryptionZoneForPath(INodesInPath
			 iip)
		{
			System.Diagnostics.Debug.Assert(dir.HasReadLock());
			Preconditions.CheckNotNull(iip);
			IList<INode> inodes = iip.GetReadOnlyINodes();
			for (int i = inodes.Count - 1; i >= 0; i--)
			{
				INode inode = inodes[i];
				if (inode != null)
				{
					EncryptionZoneManager.EncryptionZoneInt ezi = encryptionZones[inode.GetId()];
					if (ezi != null)
					{
						return ezi;
					}
				}
			}
			return null;
		}

		/// <summary>Returns an EncryptionZone representing the ez for a given path.</summary>
		/// <remarks>
		/// Returns an EncryptionZone representing the ez for a given path.
		/// Returns an empty marker EncryptionZone if path is not in an ez.
		/// </remarks>
		/// <param name="iip">The INodesInPath of the path to check</param>
		/// <returns>the EncryptionZone representing the ez for the path.</returns>
		internal virtual EncryptionZone GetEZINodeForPath(INodesInPath iip)
		{
			EncryptionZoneManager.EncryptionZoneInt ezi = GetEncryptionZoneForPath(iip);
			if (ezi == null)
			{
				return null;
			}
			else
			{
				return new EncryptionZone(ezi.GetINodeId(), GetFullPathName(ezi), ezi.GetSuite(), 
					ezi.GetVersion(), ezi.GetKeyName());
			}
		}

		/// <summary>
		/// Throws an exception if the provided path cannot be renamed into the
		/// destination because of differing encryption zones.
		/// </summary>
		/// <remarks>
		/// Throws an exception if the provided path cannot be renamed into the
		/// destination because of differing encryption zones.
		/// <p/>
		/// Called while holding the FSDirectory lock.
		/// </remarks>
		/// <param name="srcIIP">source IIP</param>
		/// <param name="dstIIP">destination IIP</param>
		/// <param name="src">source path, used for debugging</param>
		/// <exception cref="System.IO.IOException">if the src cannot be renamed to the dst</exception>
		internal virtual void CheckMoveValidity(INodesInPath srcIIP, INodesInPath dstIIP, 
			string src)
		{
			System.Diagnostics.Debug.Assert(dir.HasReadLock());
			EncryptionZoneManager.EncryptionZoneInt srcEZI = GetEncryptionZoneForPath(srcIIP);
			EncryptionZoneManager.EncryptionZoneInt dstEZI = GetEncryptionZoneForPath(dstIIP);
			bool srcInEZ = (srcEZI != null);
			bool dstInEZ = (dstEZI != null);
			if (srcInEZ)
			{
				if (!dstInEZ)
				{
					if (srcEZI.GetINodeId() == srcIIP.GetLastINode().GetId())
					{
						// src is ez root and dest is not in an ez. Allow the rename.
						return;
					}
					throw new IOException(src + " can't be moved from an encryption zone.");
				}
			}
			else
			{
				if (dstInEZ)
				{
					throw new IOException(src + " can't be moved into an encryption zone.");
				}
			}
			if (srcInEZ)
			{
				if (srcEZI != dstEZI)
				{
					string srcEZPath = GetFullPathName(srcEZI);
					string dstEZPath = GetFullPathName(dstEZI);
					StringBuilder sb = new StringBuilder(src);
					sb.Append(" can't be moved from encryption zone ");
					sb.Append(srcEZPath);
					sb.Append(" to encryption zone ");
					sb.Append(dstEZPath);
					sb.Append(".");
					throw new IOException(sb.ToString());
				}
			}
		}

		/// <summary>Create a new encryption zone.</summary>
		/// <remarks>
		/// Create a new encryption zone.
		/// <p/>
		/// Called while holding the FSDirectory lock.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual XAttr CreateEncryptionZone(string src, CipherSuite suite, CryptoProtocolVersion
			 version, string keyName)
		{
			System.Diagnostics.Debug.Assert(dir.HasWriteLock());
			INodesInPath srcIIP = dir.GetINodesInPath4Write(src, false);
			if (dir.IsNonEmptyDirectory(srcIIP))
			{
				throw new IOException("Attempt to create an encryption zone for a non-empty directory."
					);
			}
			if (srcIIP != null && srcIIP.GetLastINode() != null && !srcIIP.GetLastINode().IsDirectory
				())
			{
				throw new IOException("Attempt to create an encryption zone for a file.");
			}
			EncryptionZoneManager.EncryptionZoneInt ezi = GetEncryptionZoneForPath(srcIIP);
			if (ezi != null)
			{
				throw new IOException("Directory " + src + " is already in an " + "encryption zone. ("
					 + GetFullPathName(ezi) + ")");
			}
			HdfsProtos.ZoneEncryptionInfoProto proto = PBHelper.Convert(suite, version, keyName
				);
			XAttr ezXAttr = XAttrHelper.BuildXAttr(HdfsServerConstants.CryptoXattrEncryptionZone
				, proto.ToByteArray());
			IList<XAttr> xattrs = Lists.NewArrayListWithCapacity(1);
			xattrs.AddItem(ezXAttr);
			// updating the xattr will call addEncryptionZone,
			// done this way to handle edit log loading
			FSDirXAttrOp.UnprotectedSetXAttrs(dir, src, xattrs, EnumSet.Of(XAttrSetFlag.Create
				));
			return ezXAttr;
		}

		/// <summary>Cursor-based listing of encryption zones.</summary>
		/// <remarks>
		/// Cursor-based listing of encryption zones.
		/// <p/>
		/// Called while holding the FSDirectory lock.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual BatchedRemoteIterator.BatchedListEntries<EncryptionZone> ListEncryptionZones
			(long prevId)
		{
			System.Diagnostics.Debug.Assert(dir.HasReadLock());
			NavigableMap<long, EncryptionZoneManager.EncryptionZoneInt> tailMap = encryptionZones
				.TailMap(prevId, false);
			int numResponses = Math.Min(maxListEncryptionZonesResponses, tailMap.Count);
			IList<EncryptionZone> zones = Lists.NewArrayListWithExpectedSize(numResponses);
			int count = 0;
			foreach (EncryptionZoneManager.EncryptionZoneInt ezi in tailMap.Values)
			{
				/*
				Skip EZs that are only present in snapshots. Re-resolve the path to
				see if the path's current inode ID matches EZ map's INode ID.
				
				INode#getFullPathName simply calls getParent recursively, so will return
				the INode's parents at the time it was snapshotted. It will not
				contain a reference INode.
				*/
				string pathName = GetFullPathName(ezi);
				INodesInPath iip = dir.GetINodesInPath(pathName, false);
				INode lastINode = iip.GetLastINode();
				if (lastINode == null || lastINode.GetId() != ezi.GetINodeId())
				{
					continue;
				}
				// Add the EZ to the result list
				zones.AddItem(new EncryptionZone(ezi.GetINodeId(), pathName, ezi.GetSuite(), ezi.
					GetVersion(), ezi.GetKeyName()));
				count++;
				if (count >= numResponses)
				{
					break;
				}
			}
			bool hasMore = (numResponses < tailMap.Count);
			return new BatchedRemoteIterator.BatchedListEntries<EncryptionZone>(zones, hasMore
				);
		}
	}
}
