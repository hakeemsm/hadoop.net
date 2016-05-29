using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response sent by the <code>ResourceManager</code> to a
	/// <code>ApplicationMaster</code> on it's completion.
	/// </summary>
	/// <remarks>
	/// The response sent by the <code>ResourceManager</code> to a
	/// <code>ApplicationMaster</code> on it's completion.
	/// <p>
	/// The response, includes:
	/// <ul>
	/// <li>A flag which indicates that the application has successfully unregistered
	/// with the RM and the application can safely stop.</li>
	/// </ul>
	/// <p>
	/// Note: The flag indicates whether the application has successfully
	/// unregistered and is safe to stop. The application may stop after the flag is
	/// true. If the application stops before the flag is true then the RM may retry
	/// the application.
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.FinishApplicationMaster(FinishApplicationMasterRequest)
	/// 	"/>
	public abstract class FinishApplicationMasterResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static FinishApplicationMasterResponse NewInstance(bool isRemovedFromRMStateStore
			)
		{
			FinishApplicationMasterResponse response = Records.NewRecord<FinishApplicationMasterResponse
				>();
			response.SetIsUnregistered(isRemovedFromRMStateStore);
			return response;
		}

		/// <summary>
		/// Get the flag which indicates that the application has successfully
		/// unregistered with the RM and the application can safely stop.
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetIsUnregistered();

		/// <summary>
		/// Set the flag which indicates that the application has successfully
		/// unregistered with the RM and the application can safely stop.
		/// </summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetIsUnregistered(bool isUnregistered);
	}
}
