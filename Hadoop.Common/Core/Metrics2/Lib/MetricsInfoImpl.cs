using Com.Google.Common.Base;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Making implementing metric info a little easier</summary>
	internal class MetricsInfoImpl : MetricsInfo
	{
		private readonly string name;

		private readonly string description;

		internal MetricsInfoImpl(string name, string description)
		{
			this.name = Preconditions.CheckNotNull(name, "name");
			this.description = Preconditions.CheckNotNull(description, "description");
		}

		public virtual string Name()
		{
			return name;
		}

		public virtual string Description()
		{
			return description;
		}

		public override bool Equals(object obj)
		{
			if (obj is MetricsInfo)
			{
				MetricsInfo other = (MetricsInfo)obj;
				return Objects.Equal(name, other.Name()) && Objects.Equal(description, other.Description
					());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return Objects.HashCode(name, description);
		}

		public override string ToString()
		{
			return Objects.ToStringHelper(this).Add("name", name).Add("description", description
				).ToString();
		}
	}
}
