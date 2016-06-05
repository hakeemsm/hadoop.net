using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Offset parameter.</summary>
	public class OffsetParam : LongParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "offset";

		/// <summary>Default parameter value.</summary>
		public const string Default = "0";

		private static readonly LongParam.Domain Domain = new LongParam.Domain(Name);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public OffsetParam(long value)
			: base(Domain, value, 0L, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public OffsetParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}

		public virtual long GetOffset()
		{
			long offset = GetValue();
			return (offset == null) ? Sharpen.Extensions.ValueOf(0) : offset;
		}
	}
}
