using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Rename option set parameter.</summary>
	public class RenameOptionSetParam : EnumSetParam<Options.Rename>
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "renameoptions";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly EnumSetParam.Domain<Options.Rename> Domain = new EnumSetParam.Domain
			<Options.Rename>(Name, typeof(Options.Rename));

		/// <summary>Constructor.</summary>
		/// <param name="options">rename options.</param>
		public RenameOptionSetParam(params Options.Rename[] options)
			: base(Domain, ToEnumSet<Options.Rename>(options))
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public RenameOptionSetParam(string str)
			: base(Domain, Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
