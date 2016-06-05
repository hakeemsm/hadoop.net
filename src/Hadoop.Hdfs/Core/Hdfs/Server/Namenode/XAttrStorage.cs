using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>XAttrStorage is used to read and set xattrs for an inode.</summary>
	public class XAttrStorage
	{
		private static readonly IDictionary<string, string> internedNames = Maps.NewHashMap
			();

		/// <summary>Reads the existing extended attributes of an inode.</summary>
		/// <remarks>
		/// Reads the existing extended attributes of an inode. If the
		/// inode does not have an <code>XAttr</code>, then this method
		/// returns an empty list.
		/// <p/>
		/// Must be called while holding the FSDirectory read lock.
		/// </remarks>
		/// <param name="inode">INode to read</param>
		/// <param name="snapshotId"/>
		/// <returns>List<XAttr> <code>XAttr</code> list.</returns>
		public static IList<XAttr> ReadINodeXAttrs(INode inode, int snapshotId)
		{
			XAttrFeature f = inode.GetXAttrFeature(snapshotId);
			return f == null ? ImmutableList.Of<XAttr>() : f.GetXAttrs();
		}

		/// <summary>Reads the existing extended attributes of an inode.</summary>
		/// <remarks>
		/// Reads the existing extended attributes of an inode.
		/// <p/>
		/// Must be called while holding the FSDirectory read lock.
		/// </remarks>
		/// <param name="inodeAttr">INodeAttributes to read.</param>
		/// <returns>List<XAttr> <code>XAttr</code> list.</returns>
		public static IList<XAttr> ReadINodeXAttrs(INodeAttributes inodeAttr)
		{
			XAttrFeature f = inodeAttr.GetXAttrFeature();
			return f == null ? ImmutableList.Of<XAttr>() : f.GetXAttrs();
		}

		/// <summary>Update xattrs of inode.</summary>
		/// <remarks>
		/// Update xattrs of inode.
		/// <p/>
		/// Must be called while holding the FSDirectory write lock.
		/// </remarks>
		/// <param name="inode">INode to update</param>
		/// <param name="xAttrs">to update xAttrs.</param>
		/// <param name="snapshotId">id of the latest snapshot of the inode</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public static void UpdateINodeXAttrs(INode inode, IList<XAttr> xAttrs, int snapshotId
			)
		{
			if (xAttrs == null || xAttrs.IsEmpty())
			{
				if (inode.GetXAttrFeature() != null)
				{
					inode.RemoveXAttrFeature(snapshotId);
				}
				return;
			}
			// Dedupe the xAttr name and save them into a new interned list
			IList<XAttr> internedXAttrs = Lists.NewArrayListWithCapacity(xAttrs.Count);
			foreach (XAttr xAttr in xAttrs)
			{
				string name = xAttr.GetName();
				string internedName = internedNames[name];
				if (internedName == null)
				{
					internedName = name;
					internedNames[internedName] = internedName;
				}
				XAttr internedXAttr = new XAttr.Builder().SetName(internedName).SetNameSpace(xAttr
					.GetNameSpace()).SetValue(xAttr.GetValue()).Build();
				internedXAttrs.AddItem(internedXAttr);
			}
			// Save the list of interned xattrs
			ImmutableList<XAttr> newXAttrs = ImmutableList.CopyOf(internedXAttrs);
			if (inode.GetXAttrFeature() != null)
			{
				inode.RemoveXAttrFeature(snapshotId);
			}
			inode.AddXAttrFeature(new XAttrFeature(newXAttrs), snapshotId);
		}
	}
}
