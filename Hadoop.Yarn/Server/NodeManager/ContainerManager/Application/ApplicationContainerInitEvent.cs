using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	/// <summary>
	/// Event sent from
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.ContainerManagerImpl
	/// 	"/>
	/// to
	/// <see cref="ApplicationImpl"/>
	/// to
	/// request the initialization of a container. This is funneled through
	/// the Application so that the application life-cycle can be checked, and container
	/// launches can be delayed until the application is fully initialized.
	/// Once the application is initialized,
	/// <see cref="InitContainerTransition"/>
	/// simply passes this event on as a
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.ContainerInitEvent
	/// 	"/>
	/// .
	/// </summary>
	public class ApplicationContainerInitEvent : ApplicationEvent
	{
		internal readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container;

		public ApplicationContainerInitEvent(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container)
			: base(container.GetContainerId().GetApplicationAttemptId().GetApplicationId(), ApplicationEventType
				.InitContainer)
		{
			this.container = container;
		}

		internal virtual Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 GetContainer()
		{
			return container;
		}
	}
}
