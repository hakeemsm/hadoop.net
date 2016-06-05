using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Represent a new increased container accepted by Resource Manager</summary>
	public abstract class ContainerResourceIncrease
	{
		[InterfaceAudience.Public]
		public static ContainerResourceIncrease NewInstance(ContainerId existingContainerId
			, Resource targetCapability, Token token)
		{
			ContainerResourceIncrease context = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ContainerResourceIncrease>();
			context.SetContainerId(existingContainerId);
			context.SetCapability(targetCapability);
			context.SetContainerToken(token);
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

		[InterfaceAudience.Public]
		public abstract Token GetContainerToken();

		[InterfaceAudience.Public]
		public abstract void SetContainerToken(Token token);

		public override int GetHashCode()
		{
			return GetCapability().GetHashCode() + GetContainerId().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other is ContainerResourceIncrease)
			{
				ContainerResourceIncrease ctx = (ContainerResourceIncrease)other;
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
