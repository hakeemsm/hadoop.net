using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSDirXAttrOp
	{
		private static readonly XAttr KeyidXattr = XAttrHelper.BuildXAttr(HdfsServerConstants
			.CryptoXattrEncryptionZone, null);

		private static readonly XAttr UnreadableBySuperuserXattr = XAttrHelper.BuildXAttr
			(HdfsServerConstants.SecurityXattrUnreadableBySuperuser, null);

		/// <summary>Set xattr for a file or directory.</summary>
		/// <param name="src">- path on which it sets the xattr</param>
		/// <param name="xAttr">- xAttr details to set</param>
		/// <param name="flag">- xAttrs flags</param>
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus SetXAttr(FSDirectory fsd, string src, XAttr xAttr, 
			EnumSet<XAttrSetFlag> flag, bool logRetryCache)
		{
			CheckXAttrsConfigFlag(fsd);
			CheckXAttrSize(fsd, xAttr);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			XAttrPermissionFilter.CheckPermissionForApi(pc, xAttr, FSDirectory.IsReservedRawName
				(src));
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			IList<XAttr> xAttrs = Lists.NewArrayListWithCapacity(1);
			xAttrs.AddItem(xAttr);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				iip = fsd.GetINodesInPath4Write(src);
				CheckXAttrChangeAccess(fsd, iip, xAttr, pc);
				UnprotectedSetXAttrs(fsd, src, xAttrs, flag);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogSetXAttrs(src, xAttrs, logRetryCache);
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<XAttr> GetXAttrs(FSDirectory fsd, string srcArg, IList<XAttr
			> xAttrs)
		{
			string src = srcArg;
			CheckXAttrsConfigFlag(fsd);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			bool isRawPath = FSDirectory.IsReservedRawName(src);
			bool getAll = xAttrs == null || xAttrs.IsEmpty();
			if (!getAll)
			{
				XAttrPermissionFilter.CheckPermissionForApi(pc, xAttrs, isRawPath);
			}
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip = fsd.GetINodesInPath(src, true);
			if (fsd.IsPermissionEnabled())
			{
				fsd.CheckPathAccess(pc, iip, FsAction.Read);
			}
			IList<XAttr> all = FSDirXAttrOp.GetXAttrs(fsd, src);
			IList<XAttr> filteredAll = XAttrPermissionFilter.FilterXAttrsForApi(pc, all, isRawPath
				);
			if (getAll)
			{
				return filteredAll;
			}
			if (filteredAll == null || filteredAll.IsEmpty())
			{
				return null;
			}
			IList<XAttr> toGet = Lists.NewArrayListWithCapacity(xAttrs.Count);
			foreach (XAttr xAttr in xAttrs)
			{
				bool foundIt = false;
				foreach (XAttr a in filteredAll)
				{
					if (xAttr.GetNameSpace() == a.GetNameSpace() && xAttr.GetName().Equals(a.GetName(
						)))
					{
						toGet.AddItem(a);
						foundIt = true;
						break;
					}
				}
				if (!foundIt)
				{
					throw new IOException("At least one of the attributes provided was not found.");
				}
			}
			return toGet;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<XAttr> ListXAttrs(FSDirectory fsd, string src)
		{
			FSDirXAttrOp.CheckXAttrsConfigFlag(fsd);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			bool isRawPath = FSDirectory.IsReservedRawName(src);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip = fsd.GetINodesInPath(src, true);
			if (fsd.IsPermissionEnabled())
			{
				/* To access xattr names, you need EXECUTE in the owning directory. */
				fsd.CheckParentAccess(pc, iip, FsAction.Execute);
			}
			IList<XAttr> all = FSDirXAttrOp.GetXAttrs(fsd, src);
			return XAttrPermissionFilter.FilterXAttrsForApi(pc, all, isRawPath);
		}

		/// <summary>Remove an xattr for a file or directory.</summary>
		/// <param name="src">- path to remove the xattr from</param>
		/// <param name="xAttr">- xAttr to remove</param>
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus RemoveXAttr(FSDirectory fsd, string src, XAttr xAttr
			, bool logRetryCache)
		{
			FSDirXAttrOp.CheckXAttrsConfigFlag(fsd);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			XAttrPermissionFilter.CheckPermissionForApi(pc, xAttr, FSDirectory.IsReservedRawName
				(src));
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			IList<XAttr> xAttrs = Lists.NewArrayListWithCapacity(1);
			xAttrs.AddItem(xAttr);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				src = fsd.ResolvePath(pc, src, pathComponents);
				iip = fsd.GetINodesInPath4Write(src);
				CheckXAttrChangeAccess(fsd, iip, xAttr, pc);
				IList<XAttr> removedXAttrs = UnprotectedRemoveXAttrs(fsd, src, xAttrs);
				if (removedXAttrs != null && !removedXAttrs.IsEmpty())
				{
					fsd.GetEditLog().LogRemoveXAttrs(src, removedXAttrs, logRetryCache);
				}
				else
				{
					throw new IOException("No matching attributes found for remove operation");
				}
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<XAttr> UnprotectedRemoveXAttrs(FSDirectory fsd, string src, 
			IList<XAttr> toRemove)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodesInPath iip = fsd.GetINodesInPath4Write(FSDirectory.NormalizePath(src), true
				);
			INode inode = FSDirectory.ResolveLastINode(iip);
			int snapshotId = iip.GetLatestSnapshotId();
			IList<XAttr> existingXAttrs = XAttrStorage.ReadINodeXAttrs(inode);
			IList<XAttr> removedXAttrs = Lists.NewArrayListWithCapacity(toRemove.Count);
			IList<XAttr> newXAttrs = FilterINodeXAttrs(existingXAttrs, toRemove, removedXAttrs
				);
			if (existingXAttrs.Count != newXAttrs.Count)
			{
				XAttrStorage.UpdateINodeXAttrs(inode, newXAttrs, snapshotId);
				return removedXAttrs;
			}
			return null;
		}

		/// <summary>Filter XAttrs from a list of existing XAttrs.</summary>
		/// <remarks>
		/// Filter XAttrs from a list of existing XAttrs. Removes matched XAttrs from
		/// toFilter and puts them into filtered. Upon completion,
		/// toFilter contains the filter XAttrs that were not found, while
		/// fitleredXAttrs contains the XAttrs that were found.
		/// </remarks>
		/// <param name="existingXAttrs">Existing XAttrs to be filtered</param>
		/// <param name="toFilter">XAttrs to filter from the existing XAttrs</param>
		/// <param name="filtered">Return parameter, XAttrs that were filtered</param>
		/// <returns>List of XAttrs that does not contain filtered XAttrs</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		[VisibleForTesting]
		internal static IList<XAttr> FilterINodeXAttrs(IList<XAttr> existingXAttrs, IList
			<XAttr> toFilter, IList<XAttr> filtered)
		{
			if (existingXAttrs == null || existingXAttrs.IsEmpty() || toFilter == null || toFilter
				.IsEmpty())
			{
				return existingXAttrs;
			}
			// Populate a new list with XAttrs that pass the filter
			IList<XAttr> newXAttrs = Lists.NewArrayListWithCapacity(existingXAttrs.Count);
			foreach (XAttr a in existingXAttrs)
			{
				bool add = true;
				for (ListIterator<XAttr> it = toFilter.ListIterator(); it.HasNext(); )
				{
					XAttr filter = it.Next();
					Preconditions.CheckArgument(!KeyidXattr.EqualsIgnoreValue(filter), "The encryption zone xattr should never be deleted."
						);
					if (UnreadableBySuperuserXattr.EqualsIgnoreValue(filter))
					{
						throw new AccessControlException("The xattr '" + HdfsServerConstants.SecurityXattrUnreadableBySuperuser
							 + "' can not be deleted.");
					}
					if (a.EqualsIgnoreValue(filter))
					{
						add = false;
						it.Remove();
						filtered.AddItem(filter);
						break;
					}
				}
				if (add)
				{
					newXAttrs.AddItem(a);
				}
			}
			return newXAttrs;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static INode UnprotectedSetXAttrs(FSDirectory fsd, string src, IList<XAttr
			> xAttrs, EnumSet<XAttrSetFlag> flag)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodesInPath iip = fsd.GetINodesInPath4Write(FSDirectory.NormalizePath(src), true
				);
			INode inode = FSDirectory.ResolveLastINode(iip);
			int snapshotId = iip.GetLatestSnapshotId();
			IList<XAttr> existingXAttrs = XAttrStorage.ReadINodeXAttrs(inode);
			IList<XAttr> newXAttrs = SetINodeXAttrs(fsd, existingXAttrs, xAttrs, flag);
			bool isFile = inode.IsFile();
			foreach (XAttr xattr in newXAttrs)
			{
				string xaName = XAttrHelper.GetPrefixName(xattr);
				/*
				* If we're adding the encryption zone xattr, then add src to the list
				* of encryption zones.
				*/
				if (HdfsServerConstants.CryptoXattrEncryptionZone.Equals(xaName))
				{
					HdfsProtos.ZoneEncryptionInfoProto ezProto = HdfsProtos.ZoneEncryptionInfoProto.ParseFrom
						(xattr.GetValue());
					fsd.ezManager.AddEncryptionZone(inode.GetId(), PBHelper.Convert(ezProto.GetSuite(
						)), PBHelper.Convert(ezProto.GetCryptoProtocolVersion()), ezProto.GetKeyName());
				}
				if (!isFile && HdfsServerConstants.SecurityXattrUnreadableBySuperuser.Equals(xaName
					))
				{
					throw new IOException("Can only set '" + HdfsServerConstants.SecurityXattrUnreadableBySuperuser
						 + "' on a file.");
				}
			}
			XAttrStorage.UpdateINodeXAttrs(inode, newXAttrs, snapshotId);
			return inode;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<XAttr> SetINodeXAttrs(FSDirectory fsd, IList<XAttr> existingXAttrs
			, IList<XAttr> toSet, EnumSet<XAttrSetFlag> flag)
		{
			// Check for duplicate XAttrs in toSet
			// We need to use a custom comparator, so using a HashSet is not suitable
			for (int i = 0; i < toSet.Count; i++)
			{
				for (int j = i + 1; j < toSet.Count; j++)
				{
					if (toSet[i].EqualsIgnoreValue(toSet[j]))
					{
						throw new IOException("Cannot specify the same XAttr to be set " + "more than once"
							);
					}
				}
			}
			// Count the current number of user-visible XAttrs for limit checking
			int userVisibleXAttrsNum = 0;
			// Number of user visible xAttrs
			// The XAttr list is copied to an exactly-sized array when it's stored,
			// so there's no need to size it precisely here.
			int newSize = (existingXAttrs != null) ? existingXAttrs.Count : 0;
			newSize += toSet.Count;
			IList<XAttr> xAttrs = Lists.NewArrayListWithCapacity(newSize);
			// Check if the XAttr already exists to validate with the provided flag
			foreach (XAttr xAttr in toSet)
			{
				bool exist = false;
				if (existingXAttrs != null)
				{
					foreach (XAttr a in existingXAttrs)
					{
						if (a.EqualsIgnoreValue(xAttr))
						{
							exist = true;
							break;
						}
					}
				}
				XAttrSetFlag.Validate(xAttr.GetName(), exist, flag);
				// add the new XAttr since it passed validation
				xAttrs.AddItem(xAttr);
				if (IsUserVisible(xAttr))
				{
					userVisibleXAttrsNum++;
				}
			}
			// Add the existing xattrs back in, if they weren't already set
			if (existingXAttrs != null)
			{
				foreach (XAttr existing in existingXAttrs)
				{
					bool alreadySet = false;
					foreach (XAttr set in toSet)
					{
						if (set.EqualsIgnoreValue(existing))
						{
							alreadySet = true;
							break;
						}
					}
					if (!alreadySet)
					{
						xAttrs.AddItem(existing);
						if (IsUserVisible(existing))
						{
							userVisibleXAttrsNum++;
						}
					}
				}
			}
			if (userVisibleXAttrsNum > fsd.GetInodeXAttrsLimit())
			{
				throw new IOException("Cannot add additional XAttr to inode, " + "would exceed limit of "
					 + fsd.GetInodeXAttrsLimit());
			}
			return xAttrs;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<XAttr> GetXAttrs(FSDirectory fsd, INode inode, int snapshotId
			)
		{
			fsd.ReadLock();
			try
			{
				return XAttrStorage.ReadINodeXAttrs(inode, snapshotId);
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static XAttr UnprotectedGetXAttrByName(INode inode, int snapshotId, string
			 xAttrName)
		{
			IList<XAttr> xAttrs = XAttrStorage.ReadINodeXAttrs(inode, snapshotId);
			if (xAttrs == null)
			{
				return null;
			}
			foreach (XAttr x in xAttrs)
			{
				if (XAttrHelper.GetPrefixName(x).Equals(xAttrName))
				{
					return x;
				}
			}
			return null;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private static void CheckXAttrChangeAccess(FSDirectory fsd, INodesInPath iip, XAttr
			 xAttr, FSPermissionChecker pc)
		{
			if (fsd.IsPermissionEnabled() && xAttr.GetNameSpace() == XAttr.NameSpace.User)
			{
				INode inode = iip.GetLastINode();
				if (inode != null && inode.IsDirectory() && inode.GetFsPermission().GetStickyBit(
					))
				{
					if (!pc.IsSuperUser())
					{
						fsd.CheckOwner(pc, iip);
					}
				}
				else
				{
					fsd.CheckPathAccess(pc, iip, FsAction.Write);
				}
			}
		}

		/// <summary>
		/// Verifies that the combined size of the name and value of an xattr is within
		/// the configured limit.
		/// </summary>
		/// <remarks>
		/// Verifies that the combined size of the name and value of an xattr is within
		/// the configured limit. Setting a limit of zero disables this check.
		/// </remarks>
		private static void CheckXAttrSize(FSDirectory fsd, XAttr xAttr)
		{
			if (fsd.GetXattrMaxSize() == 0)
			{
				return;
			}
			int size = Sharpen.Runtime.GetBytesForString(xAttr.GetName(), Charsets.Utf8).Length;
			if (xAttr.GetValue() != null)
			{
				size += xAttr.GetValue().Length;
			}
			if (size > fsd.GetXattrMaxSize())
			{
				throw new HadoopIllegalArgumentException("The XAttr is too big. The maximum combined size of the"
					 + " name and value is " + fsd.GetXattrMaxSize() + ", but the total size is " + 
					size);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CheckXAttrsConfigFlag(FSDirectory fsd)
		{
			if (!fsd.IsXattrsEnabled())
			{
				throw new IOException(string.Format("The XAttr operation has been rejected.  " + 
					"Support for XAttrs has been disabled by setting %s to false.", DFSConfigKeys.DfsNamenodeXattrsEnabledKey
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static IList<XAttr> GetXAttrs(FSDirectory fsd, string src)
		{
			string srcs = FSDirectory.NormalizePath(src);
			fsd.ReadLock();
			try
			{
				INodesInPath iip = fsd.GetINodesInPath(srcs, true);
				INode inode = FSDirectory.ResolveLastINode(iip);
				int snapshotId = iip.GetPathSnapshotId();
				return XAttrStorage.ReadINodeXAttrs(fsd.GetAttributes(src, inode.GetLocalNameBytes
					(), inode, snapshotId));
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}

		private static bool IsUserVisible(XAttr xAttr)
		{
			XAttr.NameSpace ns = xAttr.GetNameSpace();
			return ns == XAttr.NameSpace.User || ns == XAttr.NameSpace.Trusted;
		}
	}
}
