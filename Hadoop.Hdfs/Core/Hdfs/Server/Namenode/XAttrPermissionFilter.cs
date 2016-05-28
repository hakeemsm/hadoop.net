using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// There are four types of extended attributes <XAttr> defined by the
	/// following namespaces:
	/// <br />
	/// USER - extended user attributes: these can be assigned to files and
	/// directories to store arbitrary additional information.
	/// </summary>
	/// <remarks>
	/// There are four types of extended attributes <XAttr> defined by the
	/// following namespaces:
	/// <br />
	/// USER - extended user attributes: these can be assigned to files and
	/// directories to store arbitrary additional information. The access
	/// permissions for user attributes are defined by the file permission
	/// bits. For sticky directories, only the owner and privileged user can
	/// write attributes.
	/// <br />
	/// TRUSTED - trusted extended attributes: these are visible/accessible
	/// only to/by the super user.
	/// <br />
	/// SECURITY - extended security attributes: these are used by the HDFS
	/// core for security purposes and are not available through admin/user
	/// API.
	/// <br />
	/// SYSTEM - extended system attributes: these are used by the HDFS
	/// core and are not available through admin/user API.
	/// <br />
	/// RAW - extended system attributes: these are used for internal system
	/// attributes that sometimes need to be exposed. Like SYSTEM namespace
	/// attributes they are not visible to the user except when getXAttr/getXAttrs
	/// is called on a file or directory in the /.reserved/raw HDFS directory
	/// hierarchy. These attributes can only be accessed by the superuser.
	/// </br>
	/// </remarks>
	public class XAttrPermissionFilter
	{
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal static void CheckPermissionForApi(FSPermissionChecker pc, XAttr xAttr, bool
			 isRawPath)
		{
			bool isSuperUser = pc.IsSuperUser();
			if (xAttr.GetNameSpace() == XAttr.NameSpace.User || (xAttr.GetNameSpace() == XAttr.NameSpace
				.Trusted && isSuperUser))
			{
				return;
			}
			if (xAttr.GetNameSpace() == XAttr.NameSpace.Raw && isRawPath && isSuperUser)
			{
				return;
			}
			if (XAttrHelper.GetPrefixName(xAttr).Equals(HdfsServerConstants.SecurityXattrUnreadableBySuperuser
				))
			{
				if (xAttr.GetValue() != null)
				{
					throw new AccessControlException("Attempt to set a value for '" + HdfsServerConstants
						.SecurityXattrUnreadableBySuperuser + "'. Values are not allowed for this xattr."
						);
				}
				return;
			}
			throw new AccessControlException("User doesn't have permission for xattr: " + XAttrHelper
				.GetPrefixName(xAttr));
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal static void CheckPermissionForApi(FSPermissionChecker pc, IList<XAttr> xAttrs
			, bool isRawPath)
		{
			Preconditions.CheckArgument(xAttrs != null);
			if (xAttrs.IsEmpty())
			{
				return;
			}
			foreach (XAttr xAttr in xAttrs)
			{
				CheckPermissionForApi(pc, xAttr, isRawPath);
			}
		}

		internal static IList<XAttr> FilterXAttrsForApi(FSPermissionChecker pc, IList<XAttr
			> xAttrs, bool isRawPath)
		{
			System.Diagnostics.Debug.Assert(xAttrs != null, "xAttrs can not be null");
			if (xAttrs.IsEmpty())
			{
				return xAttrs;
			}
			IList<XAttr> filteredXAttrs = Lists.NewArrayListWithCapacity(xAttrs.Count);
			bool isSuperUser = pc.IsSuperUser();
			foreach (XAttr xAttr in xAttrs)
			{
				if (xAttr.GetNameSpace() == XAttr.NameSpace.User)
				{
					filteredXAttrs.AddItem(xAttr);
				}
				else
				{
					if (xAttr.GetNameSpace() == XAttr.NameSpace.Trusted && isSuperUser)
					{
						filteredXAttrs.AddItem(xAttr);
					}
					else
					{
						if (xAttr.GetNameSpace() == XAttr.NameSpace.Raw && isSuperUser && isRawPath)
						{
							filteredXAttrs.AddItem(xAttr);
						}
						else
						{
							if (XAttrHelper.GetPrefixName(xAttr).Equals(HdfsServerConstants.SecurityXattrUnreadableBySuperuser
								))
							{
								filteredXAttrs.AddItem(xAttr);
							}
						}
					}
				}
			}
			return filteredXAttrs;
		}
	}
}
