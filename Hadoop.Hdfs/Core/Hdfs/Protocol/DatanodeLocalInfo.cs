using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Locally available datanode information</summary>
	public class DatanodeLocalInfo
	{
		private readonly string softwareVersion;

		private readonly string configVersion;

		private readonly long uptime;

		public DatanodeLocalInfo(string softwareVersion, string configVersion, long uptime
			)
		{
			// datanode uptime in seconds.
			this.softwareVersion = softwareVersion;
			this.configVersion = configVersion;
			this.uptime = uptime;
		}

		/// <summary>get software version</summary>
		public virtual string GetSoftwareVersion()
		{
			return this.softwareVersion;
		}

		/// <summary>get config version</summary>
		public virtual string GetConfigVersion()
		{
			return this.configVersion;
		}

		/// <summary>get uptime</summary>
		public virtual long GetUptime()
		{
			return this.uptime;
		}

		/// <summary>A formatted string for printing the status of the DataNode.</summary>
		public virtual string GetDatanodeLocalReport()
		{
			StringBuilder buffer = new StringBuilder();
			buffer.Append("Uptime: " + GetUptime());
			buffer.Append(", Software version: " + GetSoftwareVersion());
			buffer.Append(", Config version: " + GetConfigVersion());
			return buffer.ToString();
		}
	}
}
