using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Modification time parameter.</summary>
	public class ModificationTimeParam : LongParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "modificationtime";

		/// <summary>Default parameter value.</summary>
		public const string Default = "-1";

		private static readonly LongParam.Domain Domain = new LongParam.Domain(Name);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public ModificationTimeParam(long value)
			: base(Domain, value, -1L, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public ModificationTimeParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
