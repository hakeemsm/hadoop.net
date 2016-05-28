using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Replication parameter.</summary>
	public class ReplicationParam : ShortParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "replication";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static readonly ShortParam.Domain Domain = new ShortParam.Domain(Name);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public ReplicationParam(short value)
			: base(Domain, value, (short)1, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public ReplicationParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}

		/// <returns>the value or, if it is null, return the default from conf.</returns>
		public virtual short GetValue(Configuration conf)
		{
			return GetValue() != null ? GetValue() : (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey
				, DFSConfigKeys.DfsReplicationDefault);
		}
	}
}
