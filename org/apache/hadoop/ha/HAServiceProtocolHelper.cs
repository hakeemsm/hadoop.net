using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// Helper for making
	/// <see cref="HAServiceProtocol"/>
	/// RPC calls. This helper
	/// unwraps the
	/// <see cref="org.apache.hadoop.ipc.RemoteException"/>
	/// to specific exceptions.
	/// </summary>
	public class HAServiceProtocolHelper
	{
		/// <exception cref="System.IO.IOException"/>
		public static void monitorHealth(org.apache.hadoop.ha.HAServiceProtocol svc, org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
			 reqInfo)
		{
			try
			{
				svc.monitorHealth();
			}
			catch (org.apache.hadoop.ipc.RemoteException e)
			{
				throw e.unwrapRemoteException(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.HealthCheckFailedException
					)));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void transitionToActive(org.apache.hadoop.ha.HAServiceProtocol svc, 
			org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo reqInfo)
		{
			try
			{
				svc.transitionToActive(reqInfo);
			}
			catch (org.apache.hadoop.ipc.RemoteException e)
			{
				throw e.unwrapRemoteException(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.ServiceFailedException
					)));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void transitionToStandby(org.apache.hadoop.ha.HAServiceProtocol svc
			, org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo reqInfo)
		{
			try
			{
				svc.transitionToStandby(reqInfo);
			}
			catch (org.apache.hadoop.ipc.RemoteException e)
			{
				throw e.unwrapRemoteException(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.ServiceFailedException
					)));
			}
		}
	}
}
