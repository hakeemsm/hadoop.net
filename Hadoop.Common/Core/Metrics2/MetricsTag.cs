using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>Immutable tag for metrics (for grouping on host/queue/username etc.)</summary>
	public class MetricsTag : MetricsInfo
	{
		private readonly MetricsInfo info;

		private readonly string value;

		/// <summary>Construct the tag with name, description and value</summary>
		/// <param name="info">of the tag</param>
		/// <param name="value">of the tag</param>
		public MetricsTag(MetricsInfo info, string value)
		{
			this.info = Preconditions.CheckNotNull(info, "tag info");
			this.value = value;
		}

		public virtual string Name()
		{
			return info.Name();
		}

		public virtual string Description()
		{
			return info.Description();
		}

		/// <returns>the info object of the tag</returns>
		public virtual MetricsInfo Info()
		{
			return info;
		}

		/// <summary>Get the value of the tag</summary>
		/// <returns>the value</returns>
		public virtual string Value()
		{
			return value;
		}

		public override bool Equals(object obj)
		{
			if (obj is Org.Apache.Hadoop.Metrics2.MetricsTag)
			{
				Org.Apache.Hadoop.Metrics2.MetricsTag other = (Org.Apache.Hadoop.Metrics2.MetricsTag
					)obj;
				return Objects.Equal(info, other.Info()) && Objects.Equal(value, other.Value());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return Objects.HashCode(info, value);
		}

		public override string ToString()
		{
			return Objects.ToStringHelper(this).Add("info", info).Add("value", Value()).ToString
				();
		}
	}
}
