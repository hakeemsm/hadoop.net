using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	public interface LCEResourcesHandler : Configurable
	{
		/// <exception cref="System.IO.IOException"/>
		void Init(LinuxContainerExecutor lce);

		/// <summary>
		/// Called by the LinuxContainerExecutor before launching the executable
		/// inside the container.
		/// </summary>
		/// <param name="containerId">the id of the container being launched</param>
		/// <param name="containerResource">the node resources the container will be using</param>
		/// <exception cref="System.IO.IOException"/>
		void PreExecute(ContainerId containerId, Resource containerResource);

		/// <summary>
		/// Called by the LinuxContainerExecutor after the executable inside the
		/// container has exited (successfully or not).
		/// </summary>
		/// <param name="containerId">the id of the container which was launched</param>
		void PostExecute(ContainerId containerId);

		string GetResourcesOption(ContainerId containerId);
	}
}
