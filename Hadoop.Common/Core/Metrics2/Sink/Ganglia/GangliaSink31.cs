using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Sink.Ganglia
{
	/// <summary>This code supports Ganglia 3.1</summary>
	public class GangliaSink31 : GangliaSink30
	{
		public readonly Log Log = LogFactory.GetLog(this.GetType());

		/// <summary>The method sends metrics to Ganglia servers.</summary>
		/// <remarks>
		/// The method sends metrics to Ganglia servers. The method has been taken from
		/// org.apache.hadoop.metrics.ganglia.GangliaContext31 with minimal changes in
		/// order to keep it in sync.
		/// </remarks>
		/// <param name="groupName">The group name of the metric</param>
		/// <param name="name">The metric name</param>
		/// <param name="type">The type of the metric</param>
		/// <param name="value">The value of the metric</param>
		/// <param name="gConf">The GangliaConf for this metric</param>
		/// <param name="gSlope">The slope for this metric</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void EmitMetric(string groupName, string name, string
			 type, string value, GangliaConf gConf, AbstractGangliaSink.GangliaSlope gSlope)
		{
			if (name == null)
			{
				Log.Warn("Metric was emitted with no name.");
				return;
			}
			else
			{
				if (value == null)
				{
					Log.Warn("Metric name " + name + " was emitted with a null value.");
					return;
				}
				else
				{
					if (type == null)
					{
						Log.Warn("Metric name " + name + ", value " + value + " has no type.");
						return;
					}
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Emitting metric " + name + ", type " + type + ", value " + value + ", slope "
					 + gSlope.ToString() + " from hostname " + GetHostName());
			}
			// The following XDR recipe was done through a careful reading of
			// gm_protocol.x in Ganglia 3.1 and carefully examining the output of
			// the gmetric utility with strace.
			// First we send out a metadata message
			Xdr_int(128);
			// metric_id = metadata_msg
			Xdr_string(GetHostName());
			// hostname
			Xdr_string(name);
			// metric name
			Xdr_int(0);
			// spoof = False
			Xdr_string(type);
			// metric type
			Xdr_string(name);
			// metric name
			Xdr_string(gConf.GetUnits());
			// units
			Xdr_int((int)(gSlope));
			// slope
			Xdr_int(gConf.GetTmax());
			// tmax, the maximum time between metrics
			Xdr_int(gConf.GetDmax());
			// dmax, the maximum data value
			Xdr_int(1);
			/*Num of the entries in extra_value field for
			Ganglia 3.1.x*/
			Xdr_string("GROUP");
			/*Group attribute*/
			Xdr_string(groupName);
			/*Group value*/
			// send the metric to Ganglia hosts
			EmitToGangliaHosts();
			// Now we send out a message with the actual value.
			// Technically, we only need to send out the metadata message once for
			// each metric, but I don't want to have to record which metrics we did and
			// did not send.
			Xdr_int(133);
			// we are sending a string value
			Xdr_string(GetHostName());
			// hostName
			Xdr_string(name);
			// metric name
			Xdr_int(0);
			// spoof = False
			Xdr_string("%s");
			// format field
			Xdr_string(value);
			// metric value
			// send the metric to Ganglia hosts
			EmitToGangliaHosts();
		}
	}
}
