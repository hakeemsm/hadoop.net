using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// The priority assigned to a ResourceRequest or Application or Container
	/// allocation
	/// </summary>
	public abstract class Priority : Comparable<Priority>
	{
		public static readonly Priority Undefined = NewInstance(-1);

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static Priority NewInstance(int p)
		{
			Priority priority = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Priority>();
			priority.SetPriority(p);
			return priority;
		}

		/// <summary>Get the assigned priority</summary>
		/// <returns>the assigned priority</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetPriority();

		/// <summary>Set the assigned priority</summary>
		/// <param name="priority">the assigned priority</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetPriority(int priority);

		public override int GetHashCode()
		{
			int prime = 517861;
			int result = 9511;
			result = prime * result + GetPriority();
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
			Priority other = (Priority)obj;
			if (GetPriority() != other.GetPriority())
			{
				return false;
			}
			return true;
		}

		public virtual int CompareTo(Priority other)
		{
			return other.GetPriority() - this.GetPriority();
		}

		public override string ToString()
		{
			return "{Priority: " + GetPriority() + "}";
		}
	}
}
