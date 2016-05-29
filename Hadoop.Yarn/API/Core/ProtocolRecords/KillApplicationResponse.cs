using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response sent by the <code>ResourceManager</code> to the client aborting
	/// a submitted application.
	/// </summary>
	/// <remarks>
	/// The response sent by the <code>ResourceManager</code> to the client aborting
	/// a submitted application.
	/// <p>
	/// The response, includes:
	/// <ul>
	/// <li>
	/// A flag which indicates that the process of killing the application is
	/// completed or not.
	/// </li>
	/// </ul>
	/// Note: user is recommended to wait until this flag becomes true, otherwise if
	/// the <code>ResourceManager</code> crashes before the process of killing the
	/// application is completed, the <code>ResourceManager</code> may retry this
	/// application on recovery.
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.ForceKillApplication(KillApplicationRequest)
	/// 	"/>
	public abstract class KillApplicationResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static KillApplicationResponse NewInstance(bool isKillCompleted)
		{
			KillApplicationResponse response = Records.NewRecord<KillApplicationResponse>();
			response.SetIsKillCompleted(isKillCompleted);
			return response;
		}

		/// <summary>Get the flag which indicates that the process of killing application is completed or not.
		/// 	</summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetIsKillCompleted();

		/// <summary>Set the flag which indicates that the process of killing application is completed or not.
		/// 	</summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetIsKillCompleted(bool isKillCompleted);
	}
}
