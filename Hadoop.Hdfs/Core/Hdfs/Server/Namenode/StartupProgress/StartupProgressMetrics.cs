using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>
	/// Links
	/// <see cref="StartupProgress"/>
	/// to a
	/// <see cref="Org.Apache.Hadoop.Metrics2.MetricsSource"/>
	/// to expose its
	/// information via JMX.
	/// </summary>
	public class StartupProgressMetrics : MetricsSource
	{
		private static readonly MetricsInfo StartupProgressMetricsInfo = Interns.Info("StartupProgress"
			, "NameNode startup progress");

		private readonly StartupProgress startupProgress;

		/// <summary>Registers StartupProgressMetrics linked to the given StartupProgress.</summary>
		/// <param name="prog">StartupProgress to link</param>
		public static void Register(StartupProgress prog)
		{
			new Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress.StartupProgressMetrics
				(prog);
		}

		/// <summary>Creates a new StartupProgressMetrics registered with the metrics system.
		/// 	</summary>
		/// <param name="startupProgress">StartupProgress to link</param>
		public StartupProgressMetrics(StartupProgress startupProgress)
		{
			this.startupProgress = startupProgress;
			DefaultMetricsSystem.Instance().Register(StartupProgressMetricsInfo.Name(), StartupProgressMetricsInfo
				.Description(), this);
		}

		public virtual void GetMetrics(MetricsCollector collector, bool all)
		{
			StartupProgressView prog = startupProgress.CreateView();
			MetricsRecordBuilder builder = collector.AddRecord(StartupProgressMetricsInfo);
			builder.AddCounter(Interns.Info("ElapsedTime", "overall elapsed time"), prog.GetElapsedTime
				());
			builder.AddGauge(Interns.Info("PercentComplete", "overall percent complete"), prog
				.GetPercentComplete());
			foreach (Phase phase in prog.GetPhases())
			{
				AddCounter(builder, phase, "Count", " count", prog.GetCount(phase));
				AddCounter(builder, phase, "ElapsedTime", " elapsed time", prog.GetElapsedTime(phase
					));
				AddCounter(builder, phase, "Total", " total", prog.GetTotal(phase));
				AddGauge(builder, phase, "PercentComplete", " percent complete", prog.GetPercentComplete
					(phase));
			}
		}

		/// <summary>
		/// Adds a counter with a name built by using the specified phase's name as
		/// prefix and then appending the specified suffix.
		/// </summary>
		/// <param name="builder">MetricsRecordBuilder to receive counter</param>
		/// <param name="phase">Phase to add</param>
		/// <param name="nameSuffix">String suffix of metric name</param>
		/// <param name="descSuffix">String suffix of metric description</param>
		/// <param name="value">long counter value</param>
		private static void AddCounter(MetricsRecordBuilder builder, Phase phase, string 
			nameSuffix, string descSuffix, long value)
		{
			MetricsInfo metricsInfo = Interns.Info(phase.GetName() + nameSuffix, phase.GetDescription
				() + descSuffix);
			builder.AddCounter(metricsInfo, value);
		}

		/// <summary>
		/// Adds a gauge with a name built by using the specified phase's name as prefix
		/// and then appending the specified suffix.
		/// </summary>
		/// <param name="builder">MetricsRecordBuilder to receive counter</param>
		/// <param name="phase">Phase to add</param>
		/// <param name="nameSuffix">String suffix of metric name</param>
		/// <param name="descSuffix">String suffix of metric description</param>
		/// <param name="value">float gauge value</param>
		private static void AddGauge(MetricsRecordBuilder builder, Phase phase, string nameSuffix
			, string descSuffix, float value)
		{
			MetricsInfo metricsInfo = Interns.Info(phase.GetName() + nameSuffix, phase.GetDescription
				() + descSuffix);
			builder.AddGauge(metricsInfo, value);
		}
	}
}
