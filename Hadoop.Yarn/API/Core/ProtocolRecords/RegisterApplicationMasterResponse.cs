using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response sent by the
	/// <c>ResourceManager</c>
	/// to a new
	/// <c>ApplicationMaster</c>
	/// on registration.
	/// <p>
	/// The response contains critical details such as:
	/// <ul>
	/// <li>Maximum capability for allocated resources in the cluster.</li>
	/// <li>
	/// <c>ApplicationACL</c>
	/// s for the application.</li>
	/// <li>ClientToAMToken master key.</li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.RegisterApplicationMaster(RegisterApplicationMasterRequest)
	/// 	"/>
	public abstract class RegisterApplicationMasterResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RegisterApplicationMasterResponse NewInstance(Resource minCapability
			, Resource maxCapability, IDictionary<ApplicationAccessType, string> acls, ByteBuffer
			 key, IList<Container> containersFromPreviousAttempt, string queue, IList<NMToken
			> nmTokensFromPreviousAttempts)
		{
			RegisterApplicationMasterResponse response = Org.Apache.Hadoop.Yarn.Util.Records.
				NewRecord<RegisterApplicationMasterResponse>();
			response.SetMaximumResourceCapability(maxCapability);
			response.SetApplicationACLs(acls);
			response.SetClientToAMTokenMasterKey(key);
			response.SetContainersFromPreviousAttempts(containersFromPreviousAttempt);
			response.SetNMTokensFromPreviousAttempts(nmTokensFromPreviousAttempts);
			response.SetQueue(queue);
			return response;
		}

		/// <summary>
		/// Get the maximum capability for any
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// allocated by the
		/// <code>ResourceManager</code> in the cluster.
		/// </summary>
		/// <returns>maximum capability of allocated resources in the cluster</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetMaximumResourceCapability();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetMaximumResourceCapability(Resource capability);

		/// <summary>Get the <code>ApplicationACL</code>s for the application.</summary>
		/// <returns>all the <code>ApplicationACL</code>s</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IDictionary<ApplicationAccessType, string> GetApplicationACLs();

		/// <summary>Set the <code>ApplicationACL</code>s for the application.</summary>
		/// <param name="acls"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationACLs(IDictionary<ApplicationAccessType, string
			> acls);

		/// <summary>
		/// <p>Get ClientToAMToken master key.</p>
		/// <p>The ClientToAMToken master key is sent to <code>ApplicationMaster</code>
		/// by <code>ResourceManager</code> via
		/// <see cref="RegisterApplicationMasterResponse"/>
		/// , used to verify corresponding ClientToAMToken.</p>
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ByteBuffer GetClientToAMTokenMasterKey();

		/// <summary>Set ClientToAMToken master key.</summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetClientToAMTokenMasterKey(ByteBuffer key);

		/// <summary><p>Get the queue that the application was placed in.<p></summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetQueue();

		/// <summary><p>Set the queue that the application was placed in.<p></summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetQueue(string queue);

		/// <summary>
		/// <p>
		/// Get the list of running containers as viewed by
		/// <code>ResourceManager</code> from previous application attempts.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get the list of running containers as viewed by
		/// <code>ResourceManager</code> from previous application attempts.
		/// </p>
		/// </remarks>
		/// <returns>
		/// the list of running containers as viewed by
		/// <code>ResourceManager</code> from previous application attempts
		/// </returns>
		/// <seealso cref="GetNMTokensFromPreviousAttempts()"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract IList<Container> GetContainersFromPreviousAttempts();

		/// <summary>
		/// Set the list of running containers as viewed by
		/// <code>ResourceManager</code> from previous application attempts.
		/// </summary>
		/// <param name="containersFromPreviousAttempt">
		/// the list of running containers as viewed by
		/// <code>ResourceManager</code> from previous application attempts.
		/// </param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetContainersFromPreviousAttempts(IList<Container> containersFromPreviousAttempt
			);

		/// <summary>
		/// Get the list of NMTokens for communicating with the NMs where the
		/// containers of previous application attempts are running.
		/// </summary>
		/// <returns>
		/// the list of NMTokens for communicating with the NMs where the
		/// containers of previous application attempts are running.
		/// </returns>
		/// <seealso cref="GetContainersFromPreviousAttempts()"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<NMToken> GetNMTokensFromPreviousAttempts();

		/// <summary>
		/// Set the list of NMTokens for communicating with the NMs where the the
		/// containers of previous application attempts are running.
		/// </summary>
		/// <param name="nmTokens">
		/// the list of NMTokens for communicating with the NMs where the
		/// containers of previous application attempts are running.
		/// </param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNMTokensFromPreviousAttempts(IList<NMToken> nmTokens);

		/// <summary>Get a set of the resource types considered by the scheduler.</summary>
		/// <returns>a Map of RM settings</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract EnumSet<YarnServiceProtos.SchedulerResourceTypes> GetSchedulerResourceTypes
			();

		/// <summary>Set the resource types used by the scheduler.</summary>
		/// <param name="types">
		/// a set of the resource types that the scheduler considers during
		/// scheduling
		/// </param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetSchedulerResourceTypes(EnumSet<YarnServiceProtos.SchedulerResourceTypes
			> types);
	}
}
