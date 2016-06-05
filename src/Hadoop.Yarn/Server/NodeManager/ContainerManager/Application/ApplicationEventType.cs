using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	public enum ApplicationEventType
	{
		InitApplication,
		InitContainer,
		FinishApplication,
		ApplicationInited,
		ApplicationResourcesCleanedup,
		ApplicationContainerFinished,
		ApplicationLogHandlingInited,
		ApplicationLogHandlingFinished,
		ApplicationLogHandlingFailed
	}
}
