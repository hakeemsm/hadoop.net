using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Sink.Ganglia
{
	/// <summary>class which is used to store ganglia properties</summary>
	internal class GangliaConf
	{
		private string units = AbstractGangliaSink.DefaultUnits;

		private AbstractGangliaSink.GangliaSlope slope;

		private int dmax = AbstractGangliaSink.DefaultDmax;

		private int tmax = AbstractGangliaSink.DefaultTmax;

		public override string ToString()
		{
			StringBuilder buf = new StringBuilder();
			buf.Append("unit=").Append(units).Append(", slope=").Append(slope).Append(", dmax="
				).Append(dmax).Append(", tmax=").Append(tmax);
			return buf.ToString();
		}

		/// <returns>the units</returns>
		internal virtual string GetUnits()
		{
			return units;
		}

		/// <param name="units">the units to set</param>
		internal virtual void SetUnits(string units)
		{
			this.units = units;
		}

		/// <returns>the slope</returns>
		internal virtual AbstractGangliaSink.GangliaSlope GetSlope()
		{
			return slope;
		}

		/// <param name="slope">the slope to set</param>
		internal virtual void SetSlope(AbstractGangliaSink.GangliaSlope slope)
		{
			this.slope = slope;
		}

		/// <returns>the dmax</returns>
		internal virtual int GetDmax()
		{
			return dmax;
		}

		/// <param name="dmax">the dmax to set</param>
		internal virtual void SetDmax(int dmax)
		{
			this.dmax = dmax;
		}

		/// <returns>the tmax</returns>
		internal virtual int GetTmax()
		{
			return tmax;
		}

		/// <param name="tmax">the tmax to set</param>
		internal virtual void SetTmax(int tmax)
		{
			this.tmax = tmax;
		}
	}
}
