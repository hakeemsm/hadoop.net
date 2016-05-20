using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>Making implementing metric info a little easier</summary>
	internal class MetricsInfoImpl : org.apache.hadoop.metrics2.MetricsInfo
	{
		private readonly string name;

		private readonly string description;

		internal MetricsInfoImpl(string name, string description)
		{
			this.name = com.google.common.@base.Preconditions.checkNotNull(name, "name");
			this.description = com.google.common.@base.Preconditions.checkNotNull(description
				, "description");
		}

		public virtual string name()
		{
			return name;
		}

		public virtual string description()
		{
			return description;
		}

		public override bool Equals(object obj)
		{
			if (obj is org.apache.hadoop.metrics2.MetricsInfo)
			{
				org.apache.hadoop.metrics2.MetricsInfo other = (org.apache.hadoop.metrics2.MetricsInfo
					)obj;
				return com.google.common.@base.Objects.equal(name, other.name()) && com.google.common.@base.Objects
					.equal(description, other.description());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return com.google.common.@base.Objects.hashCode(name, description);
		}

		public override string ToString()
		{
			return com.google.common.@base.Objects.toStringHelper(this).add("name", name).add
				("description", description).ToString();
		}
	}
}
