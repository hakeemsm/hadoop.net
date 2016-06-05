using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown by a NodeManager that is rejecting start-container
	/// requests via
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
	/// 	"/>
	/// for containers allocated by a previous instance of the RM.
	/// </summary>
	[System.Serializable]
	public class InvalidContainerException : YarnException
	{
		private const long serialVersionUID = 1L;

		public InvalidContainerException(string msg)
			: base(msg)
		{
		}
	}
}
