using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown on
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
	/// 	"/>
	/// API
	/// when an NM starts from scratch but has not yet connected with RM.
	/// </summary>
	[System.Serializable]
	public class NMNotYetReadyException : YarnException
	{
		private const long serialVersionUID = 1L;

		public NMNotYetReadyException(string msg)
			: base(msg)
		{
		}
	}
}
