using Com.Google.Common.Base;
using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Metrics system related metrics info instances</summary>
	[System.Serializable]
	public sealed class MsInfo : MetricsInfo
	{
		public static readonly Org.Apache.Hadoop.Metrics2.Impl.MsInfo NumActiveSources = 
			new Org.Apache.Hadoop.Metrics2.Impl.MsInfo("Number of active metrics sources");

		public static readonly Org.Apache.Hadoop.Metrics2.Impl.MsInfo NumAllSources = new 
			Org.Apache.Hadoop.Metrics2.Impl.MsInfo("Number of all registered metrics sources"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Impl.MsInfo NumActiveSinks = new 
			Org.Apache.Hadoop.Metrics2.Impl.MsInfo("Number of active metrics sinks");

		public static readonly Org.Apache.Hadoop.Metrics2.Impl.MsInfo NumAllSinks = new Org.Apache.Hadoop.Metrics2.Impl.MsInfo
			("Number of all registered metrics sinks");

		public static readonly Org.Apache.Hadoop.Metrics2.Impl.MsInfo Context = new Org.Apache.Hadoop.Metrics2.Impl.MsInfo
			("Metrics context");

		public static readonly Org.Apache.Hadoop.Metrics2.Impl.MsInfo Hostname = new Org.Apache.Hadoop.Metrics2.Impl.MsInfo
			("Local hostname");

		public static readonly Org.Apache.Hadoop.Metrics2.Impl.MsInfo SessionId = new Org.Apache.Hadoop.Metrics2.Impl.MsInfo
			("Session ID");

		public static readonly Org.Apache.Hadoop.Metrics2.Impl.MsInfo ProcessName = new Org.Apache.Hadoop.Metrics2.Impl.MsInfo
			("Process name");

		private readonly string desc;

		internal MsInfo(string desc)
		{
			this.desc = desc;
		}

		public string Description()
		{
			return Org.Apache.Hadoop.Metrics2.Impl.MsInfo.desc;
		}

		public override string ToString()
		{
			return Objects.ToStringHelper(this).Add("name", Name()).Add("description", Org.Apache.Hadoop.Metrics2.Impl.MsInfo
				.desc).ToString();
		}
	}
}
