using Sharpen;

namespace org.apache.hadoop.metrics2.sink.ganglia
{
	/// <summary>This code supports Ganglia 3.1</summary>
	public class GangliaSink31 : org.apache.hadoop.metrics2.sink.ganglia.GangliaSink30
	{
		public readonly org.apache.commons.logging.Log LOG;

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
		protected internal override void emitMetric(string groupName, string name, string
			 type, string value, org.apache.hadoop.metrics2.sink.ganglia.GangliaConf gConf, 
			org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope gSlope)
		{
			if (name == null)
			{
				LOG.warn("Metric was emitted with no name.");
				return;
			}
			else
			{
				if (value == null)
				{
					LOG.warn("Metric name " + name + " was emitted with a null value.");
					return;
				}
				else
				{
					if (type == null)
					{
						LOG.warn("Metric name " + name + ", value " + value + " has no type.");
						return;
					}
				}
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Emitting metric " + name + ", type " + type + ", value " + value + ", slope "
					 + gSlope.ToString() + " from hostname " + getHostName());
			}
			// The following XDR recipe was done through a careful reading of
			// gm_protocol.x in Ganglia 3.1 and carefully examining the output of
			// the gmetric utility with strace.
			// First we send out a metadata message
			xdr_int(128);
			// metric_id = metadata_msg
			xdr_string(getHostName());
			// hostname
			xdr_string(name);
			// metric name
			xdr_int(0);
			// spoof = False
			xdr_string(type);
			// metric type
			xdr_string(name);
			// metric name
			xdr_string(gConf.getUnits());
			// units
			xdr_int((int)(gSlope));
			// slope
			xdr_int(gConf.getTmax());
			// tmax, the maximum time between metrics
			xdr_int(gConf.getDmax());
			// dmax, the maximum data value
			xdr_int(1);
			/*Num of the entries in extra_value field for
			Ganglia 3.1.x*/
			xdr_string("GROUP");
			/*Group attribute*/
			xdr_string(groupName);
			/*Group value*/
			// send the metric to Ganglia hosts
			emitToGangliaHosts();
			// Now we send out a message with the actual value.
			// Technically, we only need to send out the metadata message once for
			// each metric, but I don't want to have to record which metrics we did and
			// did not send.
			xdr_int(133);
			// we are sending a string value
			xdr_string(getHostName());
			// hostName
			xdr_string(name);
			// metric name
			xdr_int(0);
			// spoof = False
			xdr_string("%s");
			// format field
			xdr_string(value);
			// metric value
			// send the metric to Ganglia hosts
			emitToGangliaHosts();
		}

		public GangliaSink31()
		{
			LOG = org.apache.commons.logging.LogFactory.getLog(Sharpen.Runtime.getClassForObject
				(this));
		}
	}
}
