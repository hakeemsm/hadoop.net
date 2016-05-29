using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>ContainerLaunchContext</c>
	/// represents all of the information
	/// needed by the
	/// <c>NodeManager</c>
	/// to launch a container.
	/// <p>
	/// It includes details such as:
	/// <ul>
	/// <li>
	/// <see cref="ContainerId"/>
	/// of the container.</li>
	/// <li>
	/// <see cref="Resource"/>
	/// allocated to the container.</li>
	/// <li>User to whom the container is allocated.</li>
	/// <li>Security tokens (if security is enabled).</li>
	/// <li>
	/// <see cref="LocalResource"/>
	/// necessary for running the container such
	/// as binaries, jar, shared-objects, side-files etc.
	/// </li>
	/// <li>Optional, application-specific binary service data.</li>
	/// <li>Environment variables for the launched process.</li>
	/// <li>Command to launch the container.</li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
	/// 	"/>
	public abstract class ContainerLaunchContext
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ContainerLaunchContext NewInstance(IDictionary<string, LocalResource
			> localResources, IDictionary<string, string> environment, IList<string> commands
			, IDictionary<string, ByteBuffer> serviceData, ByteBuffer tokens, IDictionary<ApplicationAccessType
			, string> acls)
		{
			ContainerLaunchContext container = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				ContainerLaunchContext>();
			container.SetLocalResources(localResources);
			container.SetEnvironment(environment);
			container.SetCommands(commands);
			container.SetServiceData(serviceData);
			container.SetTokens(tokens);
			container.SetApplicationACLs(acls);
			return container;
		}

		/// <summary>Get all the tokens needed by this container.</summary>
		/// <remarks>
		/// Get all the tokens needed by this container. It may include file-system
		/// tokens, ApplicationMaster related tokens if this container is an
		/// ApplicationMaster or framework level tokens needed by this container to
		/// communicate to various services in a secure manner.
		/// </remarks>
		/// <returns>tokens needed by this container.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ByteBuffer GetTokens();

		/// <summary>Set security tokens needed by this container.</summary>
		/// <param name="tokens">security tokens</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetTokens(ByteBuffer tokens);

		/// <summary>Get <code>LocalResource</code> required by the container.</summary>
		/// <returns>all <code>LocalResource</code> required by the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IDictionary<string, LocalResource> GetLocalResources();

		/// <summary>Set <code>LocalResource</code> required by the container.</summary>
		/// <remarks>
		/// Set <code>LocalResource</code> required by the container. All pre-existing
		/// Map entries are cleared before adding the new Map
		/// </remarks>
		/// <param name="localResources"><code>LocalResource</code> required by the container
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetLocalResources(IDictionary<string, LocalResource> localResources
			);

		/// <summary>
		/// <p>
		/// Get application-specific binary <em>service data</em>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get application-specific binary <em>service data</em>. This is a map keyed
		/// by the name of each
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.AuxiliaryService"/>
		/// that is configured on a
		/// NodeManager and value correspond to the application specific data targeted
		/// for the keyed
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.AuxiliaryService"/>
		/// .
		/// </p>
		/// <p>
		/// This will be used to initialize this application on the specific
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.AuxiliaryService"/>
		/// running on the NodeManager by calling
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.AuxiliaryService.InitializeApplication(Org.Apache.Hadoop.Yarn.Server.Api.ApplicationInitializationContext)
		/// 	"/>
		/// </p>
		/// </remarks>
		/// <returns>application-specific binary <em>service data</em></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IDictionary<string, ByteBuffer> GetServiceData();

		/// <summary>
		/// <p>
		/// Set application-specific binary <em>service data</em>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Set application-specific binary <em>service data</em>. This is a map keyed
		/// by the name of each
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.AuxiliaryService"/>
		/// that is configured on a
		/// NodeManager and value correspond to the application specific data targeted
		/// for the keyed
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.AuxiliaryService"/>
		/// . All pre-existing Map entries are
		/// preserved.
		/// </p>
		/// </remarks>
		/// <param name="serviceData">application-specific binary <em>service data</em></param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetServiceData(IDictionary<string, ByteBuffer> serviceData);

		/// <summary>Get <em>environment variables</em> for the container.</summary>
		/// <returns><em>environment variables</em> for the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IDictionary<string, string> GetEnvironment();

		/// <summary>Add <em>environment variables</em> for the container.</summary>
		/// <remarks>
		/// Add <em>environment variables</em> for the container. All pre-existing Map
		/// entries are cleared before adding the new Map
		/// </remarks>
		/// <param name="environment"><em>environment variables</em> for the container</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetEnvironment(IDictionary<string, string> environment);

		/// <summary>Get the list of <em>commands</em> for launching the container.</summary>
		/// <returns>the list of <em>commands</em> for launching the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<string> GetCommands();

		/// <summary>Add the list of <em>commands</em> for launching the container.</summary>
		/// <remarks>
		/// Add the list of <em>commands</em> for launching the container. All
		/// pre-existing List entries are cleared before adding the new List
		/// </remarks>
		/// <param name="commands">the list of <em>commands</em> for launching the container</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetCommands(IList<string> commands);

		/// <summary>Get the <code>ApplicationACL</code>s for the application.</summary>
		/// <returns>all the <code>ApplicationACL</code>s</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IDictionary<ApplicationAccessType, string> GetApplicationACLs();

		/// <summary>Set the <code>ApplicationACL</code>s for the application.</summary>
		/// <remarks>
		/// Set the <code>ApplicationACL</code>s for the application. All pre-existing
		/// Map entries are cleared before adding the new Map
		/// </remarks>
		/// <param name="acls"><code>ApplicationACL</code>s for the application</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetApplicationACLs(IDictionary<ApplicationAccessType, string
			> acls);
	}
}
