using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	public abstract class NMClient : AbstractService
	{
		/// <summary>Create a new instance of NMClient.</summary>
		[InterfaceAudience.Public]
		public static Org.Apache.Hadoop.Yarn.Client.Api.NMClient CreateNMClient()
		{
			Org.Apache.Hadoop.Yarn.Client.Api.NMClient client = new NMClientImpl();
			return client;
		}

		/// <summary>Create a new instance of NMClient.</summary>
		[InterfaceAudience.Public]
		public static Org.Apache.Hadoop.Yarn.Client.Api.NMClient CreateNMClient(string name
			)
		{
			Org.Apache.Hadoop.Yarn.Client.Api.NMClient client = new NMClientImpl(name);
			return client;
		}

		private NMTokenCache nmTokenCache = NMTokenCache.GetSingleton();

		[InterfaceAudience.Private]
		protected internal NMClient(string name)
			: base(name)
		{
		}

		/// <summary>
		/// <p>Start an allocated container.</p>
		/// <p>The <code>ApplicationMaster</code> or other applications that use the
		/// client must provide the details of the allocated container, including the
		/// Id, the assigned node's Id and the token via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
		/// . In
		/// addition, the AM needs to provide the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
		/// as
		/// well.</p>
		/// </summary>
		/// <param name="container">the allocated container</param>
		/// <param name="containerLaunchContext">
		/// the context information needed by the
		/// <code>NodeManager</code> to launch the
		/// container
		/// </param>
		/// <returns>a map between the auxiliary service names and their outputs</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IDictionary<string, ByteBuffer> StartContainer(Container container
			, ContainerLaunchContext containerLaunchContext);

		/// <summary><p>Stop an started container.</p></summary>
		/// <param name="containerId">the Id of the started container</param>
		/// <param name="nodeId">the Id of the <code>NodeManager</code></param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StopContainer(ContainerId containerId, NodeId nodeId);

		/// <summary><p>Query the status of a container.</p></summary>
		/// <param name="containerId">the Id of the started container</param>
		/// <param name="nodeId">the Id of the <code>NodeManager</code></param>
		/// <returns>the status of a container</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract ContainerStatus GetContainerStatus(ContainerId containerId, NodeId
			 nodeId);

		/// <summary>
		/// <p>Set whether the containers that are started by this client, and are
		/// still running should be stopped when the client stops.
		/// </summary>
		/// <remarks>
		/// <p>Set whether the containers that are started by this client, and are
		/// still running should be stopped when the client stops. By default, the
		/// feature should be enabled.</p> However, containers will be stopped only
		/// when service is stopped. i.e. after
		/// <see cref="Org.Apache.Hadoop.Service.AbstractService.Stop()"/>
		/// .
		/// </remarks>
		/// <param name="enabled">whether the feature is enabled or not</param>
		public abstract void CleanupRunningContainersOnStop(bool enabled);

		/// <summary>Set the NM Token cache of the <code>NMClient</code>.</summary>
		/// <remarks>
		/// Set the NM Token cache of the <code>NMClient</code>. This cache must be
		/// shared with the
		/// <see cref="AMRMClient{T}"/>
		/// that requested the containers managed
		/// by this <code>NMClient</code>
		/// <p>
		/// If a NM token cache is not set, the
		/// <see cref="NMTokenCache.GetSingleton()"/>
		/// singleton instance will be used.
		/// </remarks>
		/// <param name="nmTokenCache">the NM token cache to use.</param>
		public virtual void SetNMTokenCache(NMTokenCache nmTokenCache)
		{
			this.nmTokenCache = nmTokenCache;
		}

		/// <summary>Get the NM token cache of the <code>NMClient</code>.</summary>
		/// <remarks>
		/// Get the NM token cache of the <code>NMClient</code>. This cache must be
		/// shared with the
		/// <see cref="AMRMClient{T}"/>
		/// that requested the containers managed
		/// by this <code>NMClient</code>
		/// <p>
		/// If a NM token cache is not set, the
		/// <see cref="NMTokenCache.GetSingleton()"/>
		/// singleton instance will be used.
		/// </remarks>
		/// <returns>the NM token cache</returns>
		public virtual NMTokenCache GetNMTokenCache()
		{
			return nmTokenCache;
		}
	}
}
