using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public enum ContainerEventType
	{
		InitContainer,
		KillContainer,
		UpdateDiagnosticsMsg,
		ContainerDone,
		ContainerInited,
		ResourceLocalized,
		ResourceFailed,
		ContainerResourcesCleanedup,
		ContainerLaunched,
		ContainerExitedWithSuccess,
		ContainerExitedWithFailure,
		ContainerKilledOnRequest
	}
}
