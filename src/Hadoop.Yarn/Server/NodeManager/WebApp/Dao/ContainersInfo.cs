using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao
{
	public class ContainersInfo
	{
		protected internal AList<ContainerInfo> container = new AList<ContainerInfo>();

		public ContainersInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(ContainerInfo containerInfo)
		{
			container.AddItem(containerInfo);
		}

		public virtual AList<ContainerInfo> GetContainers()
		{
			return container;
		}
	}
}
