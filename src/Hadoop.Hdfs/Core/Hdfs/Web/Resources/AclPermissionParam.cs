using System.Collections.Generic;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>AclPermission parameter.</summary>
	public class AclPermissionParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "aclspec";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			Sharpen.Pattern.Compile(DFSConfigKeys.DfsWebhdfsAclPermissionPatternDefault));

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public AclPermissionParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : str)
		{
		}

		public AclPermissionParam(IList<AclEntry> acl)
			: base(Domain, ParseAclSpec(acl).Equals(Default) ? null : ParseAclSpec(acl))
		{
		}

		public override string GetName()
		{
			return Name;
		}

		public virtual IList<AclEntry> GetAclPermission(bool includePermission)
		{
			string v = GetValue();
			return (v != null ? AclEntry.ParseAclSpec(v, includePermission) : AclEntry.ParseAclSpec
				(Default, includePermission));
		}

		/// <returns>
		/// parse
		/// <paramref name="aclEntry"/>
		/// and return aclspec
		/// </returns>
		private static string ParseAclSpec(IList<AclEntry> aclEntry)
		{
			return StringUtils.Join(aclEntry, ",");
		}
	}
}
