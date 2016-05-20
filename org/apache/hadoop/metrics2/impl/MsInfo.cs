using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>Metrics system related metrics info instances</summary>
	[System.Serializable]
	public sealed class MsInfo : org.apache.hadoop.metrics2.MetricsInfo
	{
		public static readonly org.apache.hadoop.metrics2.impl.MsInfo NumActiveSources = 
			new org.apache.hadoop.metrics2.impl.MsInfo("Number of active metrics sources");

		public static readonly org.apache.hadoop.metrics2.impl.MsInfo NumAllSources = new 
			org.apache.hadoop.metrics2.impl.MsInfo("Number of all registered metrics sources"
			);

		public static readonly org.apache.hadoop.metrics2.impl.MsInfo NumActiveSinks = new 
			org.apache.hadoop.metrics2.impl.MsInfo("Number of active metrics sinks");

		public static readonly org.apache.hadoop.metrics2.impl.MsInfo NumAllSinks = new org.apache.hadoop.metrics2.impl.MsInfo
			("Number of all registered metrics sinks");

		public static readonly org.apache.hadoop.metrics2.impl.MsInfo Context = new org.apache.hadoop.metrics2.impl.MsInfo
			("Metrics context");

		public static readonly org.apache.hadoop.metrics2.impl.MsInfo Hostname = new org.apache.hadoop.metrics2.impl.MsInfo
			("Local hostname");

		public static readonly org.apache.hadoop.metrics2.impl.MsInfo SessionId = new org.apache.hadoop.metrics2.impl.MsInfo
			("Session ID");

		public static readonly org.apache.hadoop.metrics2.impl.MsInfo ProcessName = new org.apache.hadoop.metrics2.impl.MsInfo
			("Process name");

		private readonly string desc;

		internal MsInfo(string desc)
		{
			this.desc = desc;
		}

		public string description()
		{
			return org.apache.hadoop.metrics2.impl.MsInfo.desc;
		}

		public override string ToString()
		{
			return com.google.common.@base.Objects.toStringHelper(this).add("name", name()).add
				("description", org.apache.hadoop.metrics2.impl.MsInfo.desc).ToString();
		}
	}
}
