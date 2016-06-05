using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// Used by Application Master to ask Node Manager reduce size of a specified
	/// container
	/// </summary>
	public abstract class ContainerResourceDecrease
	{
		[InterfaceAudience.Public]
		public static ContainerResourceDecrease NewInstance(ContainerId existingContainerId
			, Resource targetCapability)
		{
			ContainerResourceDecrease context = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ContainerResourceDecrease>();
			context.SetContainerId(existingContainerId);
			context.SetCapability(targetCapability);
			return context;
		}

		[InterfaceAudience.Public]
		public abstract ContainerId GetContainerId();

		[InterfaceAudience.Public]
		public abstract void SetContainerId(ContainerId containerId);

		[InterfaceAudience.Public]
		public abstract Resource GetCapability();

		[InterfaceAudience.Public]
		public abstract void SetCapability(Resource capability);

		public override int GetHashCode()
		{
			return GetCapability().GetHashCode() + GetContainerId().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other is ContainerResourceDecrease)
			{
				ContainerResourceDecrease ctx = (ContainerResourceDecrease)other;
				if (GetContainerId() == null && ctx.GetContainerId() != null)
				{
					return false;
				}
				else
				{
					if (!GetContainerId().Equals(ctx.GetContainerId()))
					{
						return false;
					}
				}
				if (GetCapability() == null && ctx.GetCapability() != null)
				{
					return false;
				}
				else
				{
					if (!GetCapability().Equals(ctx.GetCapability()))
					{
						return false;
					}
				}
				return true;
			}
			else
			{
				return false;
			}
		}
	}
}
