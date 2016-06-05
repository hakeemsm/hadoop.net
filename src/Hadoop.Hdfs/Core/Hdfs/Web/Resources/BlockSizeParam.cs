using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Block size parameter.</summary>
	public class BlockSizeParam : LongParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "blocksize";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static readonly LongParam.Domain Domain = new LongParam.Domain(Name);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public BlockSizeParam(long value)
			: base(Domain, value, 1L, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public BlockSizeParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}

		/// <returns>the value or, if it is null, return the default from conf.</returns>
		public virtual long GetValue(Configuration conf)
		{
			return GetValue() != null ? GetValue() : conf.GetLongBytes(DFSConfigKeys.DfsBlockSizeKey
				, DFSConfigKeys.DfsBlockSizeDefault);
		}
	}
}
