using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	/// <summary>An entity in YARN that can be guarded with ACLs.</summary>
	/// <remarks>
	/// An entity in YARN that can be guarded with ACLs. The entity could be an
	/// application or a queue etc. An application entity has access types defined in
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAccessType"/>
	/// , a queue entity has access types defined in
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueACL"/>
	/// .
	/// </remarks>
	public class PrivilegedEntity
	{
		public enum EntityType
		{
			Queue
		}

		internal PrivilegedEntity.EntityType type;

		internal string name;

		public PrivilegedEntity(PrivilegedEntity.EntityType type, string name)
		{
			this.type = type;
			this.name = name;
		}

		public virtual PrivilegedEntity.EntityType GetType()
		{
			return type;
		}

		public virtual string GetName()
		{
			return name;
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.GetHashCode());
			result = prime * result + ((type == null) ? 0 : type.GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Security.PrivilegedEntity other = (Org.Apache.Hadoop.Yarn.Security.PrivilegedEntity
				)obj;
			if (name == null)
			{
				if (other.name != null)
				{
					return false;
				}
			}
			else
			{
				if (!name.Equals(other.name))
				{
					return false;
				}
			}
			if (type != other.type)
			{
				return false;
			}
			return true;
		}
	}
}
