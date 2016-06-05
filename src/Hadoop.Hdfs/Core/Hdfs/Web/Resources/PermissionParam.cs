using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Permission parameter, use a Short to represent a FsPermission.</summary>
	public class PermissionParam : ShortParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "permission";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static readonly ShortParam.Domain Domain = new ShortParam.Domain(Name, 8);

		private const short DefaultPermission = 0x1ed;

		/// <returns>the default FsPermission.</returns>
		public static FsPermission GetDefaultFsPermission()
		{
			return new FsPermission(DefaultPermission);
		}

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public PermissionParam(FsPermission value)
			: base(Domain, value == null ? null : value.ToShort(), null, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public PermissionParam(string str)
			: base(Domain, Domain.Parse(str), (short)0, (short)0x3ff)
		{
		}

		public override string GetName()
		{
			return Name;
		}

		/// <returns>the represented FsPermission.</returns>
		public virtual FsPermission GetFsPermission()
		{
			short v = GetValue();
			return new FsPermission(v != null ? v : DefaultPermission);
		}
	}
}
