using Org.Apache.Hadoop.Ipc;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Helper for making
	/// <see cref="HAServiceProtocol"/>
	/// RPC calls. This helper
	/// unwraps the
	/// <see cref="Org.Apache.Hadoop.Ipc.RemoteException"/>
	/// to specific exceptions.
	/// </summary>
	public class HAServiceProtocolHelper
	{
		/// <exception cref="System.IO.IOException"/>
		public static void MonitorHealth(HAServiceProtocol svc, HAServiceProtocol.StateChangeRequestInfo
			 reqInfo)
		{
			try
			{
				svc.MonitorHealth();
			}
			catch (RemoteException e)
			{
				throw e.UnwrapRemoteException(typeof(HealthCheckFailedException));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void TransitionToActive(HAServiceProtocol svc, HAServiceProtocol.StateChangeRequestInfo
			 reqInfo)
		{
			try
			{
				svc.TransitionToActive(reqInfo);
			}
			catch (RemoteException e)
			{
				throw e.UnwrapRemoteException(typeof(ServiceFailedException));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void TransitionToStandby(HAServiceProtocol svc, HAServiceProtocol.StateChangeRequestInfo
			 reqInfo)
		{
			try
			{
				svc.TransitionToStandby(reqInfo);
			}
			catch (RemoteException e)
			{
				throw e.UnwrapRemoteException(typeof(ServiceFailedException));
			}
		}
	}
}
