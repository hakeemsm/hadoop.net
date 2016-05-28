using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.FS.Permission.FsAction"/>
	/// Parameter
	/// </summary>
	public class FsActionParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "fsaction";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static string FsActionPattern = "[rwx-]{3}";

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			Sharpen.Pattern.Compile(FsActionPattern));

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public FsActionParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : str)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public FsActionParam(FsAction value)
			: base(Domain, value == null ? null : value.Symbol)
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
