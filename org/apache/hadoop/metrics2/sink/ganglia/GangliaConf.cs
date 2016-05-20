using Sharpen;

namespace org.apache.hadoop.metrics2.sink.ganglia
{
	/// <summary>class which is used to store ganglia properties</summary>
	internal class GangliaConf
	{
		private string units = org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink
			.DEFAULT_UNITS;

		private org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope 
			slope;

		private int dmax = org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.DEFAULT_DMAX;

		private int tmax = org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.DEFAULT_TMAX;

		public override string ToString()
		{
			java.lang.StringBuilder buf = new java.lang.StringBuilder();
			buf.Append("unit=").Append(units).Append(", slope=").Append(slope).Append(", dmax="
				).Append(dmax).Append(", tmax=").Append(tmax);
			return buf.ToString();
		}

		/// <returns>the units</returns>
		internal virtual string getUnits()
		{
			return units;
		}

		/// <param name="units">the units to set</param>
		internal virtual void setUnits(string units)
		{
			this.units = units;
		}

		/// <returns>the slope</returns>
		internal virtual org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope
			 getSlope()
		{
			return slope;
		}

		/// <param name="slope">the slope to set</param>
		internal virtual void setSlope(org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope
			 slope)
		{
			this.slope = slope;
		}

		/// <returns>the dmax</returns>
		internal virtual int getDmax()
		{
			return dmax;
		}

		/// <param name="dmax">the dmax to set</param>
		internal virtual void setDmax(int dmax)
		{
			this.dmax = dmax;
		}

		/// <returns>the tmax</returns>
		internal virtual int getTmax()
		{
			return tmax;
		}

		/// <param name="tmax">the tmax to set</param>
		internal virtual void setTmax(int tmax)
		{
			this.tmax = tmax;
		}
	}
}
