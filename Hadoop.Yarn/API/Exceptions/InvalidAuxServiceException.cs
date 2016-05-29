using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown by a NodeManager that is rejecting start-container
	/// requests via
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
	/// 	"/>
	/// for auxservices does not exist.
	/// </summary>
	[System.Serializable]
	public class InvalidAuxServiceException : YarnException
	{
		private const long serialVersionUID = 1L;

		public InvalidAuxServiceException(string msg)
			: base(msg)
		{
		}
	}
}
