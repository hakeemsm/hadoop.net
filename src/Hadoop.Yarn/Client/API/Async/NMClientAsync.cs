using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Async
{
	/// <summary>
	/// <code>NMClientAsync</code> handles communication with all the NodeManagers
	/// and provides asynchronous updates on getting responses from them.
	/// </summary>
	/// <remarks>
	/// <code>NMClientAsync</code> handles communication with all the NodeManagers
	/// and provides asynchronous updates on getting responses from them. It
	/// maintains a thread pool to communicate with individual NMs where a number of
	/// worker threads process requests to NMs by using
	/// <see cref="Org.Apache.Hadoop.Yarn.Client.Api.Impl.NMClientImpl"/>
	/// . The max
	/// size of the thread pool is configurable through
	/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.NmClientAsyncThreadPoolMaxSize
	/// 	"/>
	/// .
	/// It should be used in conjunction with a CallbackHandler. For example
	/// <pre>
	/// <c/>
	/// class MyCallbackHandler implements NMClientAsync.CallbackHandler
	/// public void onContainerStarted(ContainerId containerId,
	/// Map<String, ByteBuffer> allServiceResponse) {
	/// [post process after the container is started, process the response]
	/// }
	/// public void onContainerStatusReceived(ContainerId containerId,
	/// ContainerStatus containerStatus) {
	/// [make use of the status of the container]
	/// }
	/// public void onContainerStopped(ContainerId containerId) {
	/// [post process after the container is stopped]
	/// }
	/// public void onStartContainerError(
	/// ContainerId containerId, Throwable t) {
	/// [handle the raised exception]
	/// }
	/// public void onGetContainerStatusError(
	/// ContainerId containerId, Throwable t) {
	/// [handle the raised exception]
	/// }
	/// public void onStopContainerError(
	/// ContainerId containerId, Throwable t) {
	/// [handle the raised exception]
	/// }
	/// }
	/// }
	/// </pre>
	/// The client's life-cycle should be managed like the following:
	/// <pre>
	/// <c>
	/// NMClientAsync asyncClient =
	/// NMClientAsync.createNMClientAsync(new MyCallbackhandler());
	/// asyncClient.init(conf);
	/// asyncClient.start();
	/// asyncClient.startContainer(container, containerLaunchContext);
	/// [... wait for container being started]
	/// asyncClient.getContainerStatus(container.getId(), container.getNodeId(),
	/// container.getContainerToken());
	/// [... handle the status in the callback instance]
	/// asyncClient.stopContainer(container.getId(), container.getNodeId(),
	/// container.getContainerToken());
	/// [... wait for container being stopped]
	/// asyncClient.stop();
	/// </c>
	/// </pre>
	/// </remarks>
	public abstract class NMClientAsync : AbstractService
	{
		protected internal NMClient client;

		protected internal NMClientAsync.CallbackHandler callbackHandler;

		public static Org.Apache.Hadoop.Yarn.Client.Api.Async.NMClientAsync CreateNMClientAsync
			(NMClientAsync.CallbackHandler callbackHandler)
		{
			return new NMClientAsyncImpl(callbackHandler);
		}

		protected internal NMClientAsync(NMClientAsync.CallbackHandler callbackHandler)
			: this(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Async.NMClientAsync).FullName, callbackHandler
				)
		{
		}

		protected internal NMClientAsync(string name, NMClientAsync.CallbackHandler callbackHandler
			)
			: this(name, new NMClientImpl(), callbackHandler)
		{
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal NMClientAsync(string name, NMClient client, NMClientAsync.CallbackHandler
			 callbackHandler)
			: base(name)
		{
			this.SetClient(client);
			this.SetCallbackHandler(callbackHandler);
		}

		public abstract void StartContainerAsync(Container container, ContainerLaunchContext
			 containerLaunchContext);

		public abstract void StopContainerAsync(ContainerId containerId, NodeId nodeId);

		public abstract void GetContainerStatusAsync(ContainerId containerId, NodeId nodeId
			);

		public virtual NMClient GetClient()
		{
			return client;
		}

		public virtual void SetClient(NMClient client)
		{
			this.client = client;
		}

		public virtual NMClientAsync.CallbackHandler GetCallbackHandler()
		{
			return callbackHandler;
		}

		public virtual void SetCallbackHandler(NMClientAsync.CallbackHandler callbackHandler
			)
		{
			this.callbackHandler = callbackHandler;
		}

		/// <summary>
		/// <p>
		/// The callback interface needs to be implemented by
		/// <see cref="NMClientAsync"/>
		/// users. The APIs are called when responses from <code>NodeManager</code> are
		/// available.
		/// </p>
		/// <p>
		/// Once a callback happens, the users can chose to act on it in blocking or
		/// non-blocking manner. If the action on callback is done in a blocking
		/// manner, some of the threads performing requests on NodeManagers may get
		/// blocked depending on how many threads in the pool are busy.
		/// </p>
		/// <p>
		/// The implementation of the callback function should not throw the
		/// unexpected exception. Otherwise,
		/// <see cref="NMClientAsync"/>
		/// will just
		/// catch, log and then ignore it.
		/// </p>
		/// </summary>
		public interface CallbackHandler
		{
			/// <summary>
			/// The API is called when <code>NodeManager</code> responds to indicate its
			/// acceptance of the starting container request
			/// </summary>
			/// <param name="containerId">the Id of the container</param>
			/// <param name="allServiceResponse">
			/// a Map between the auxiliary service names and
			/// their outputs
			/// </param>
			void OnContainerStarted(ContainerId containerId, IDictionary<string, ByteBuffer> 
				allServiceResponse);

			/// <summary>
			/// The API is called when <code>NodeManager</code> responds with the status
			/// of the container
			/// </summary>
			/// <param name="containerId">the Id of the container</param>
			/// <param name="containerStatus">the status of the container</param>
			void OnContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus
				);

			/// <summary>
			/// The API is called when <code>NodeManager</code> responds to indicate the
			/// container is stopped.
			/// </summary>
			/// <param name="containerId">the Id of the container</param>
			void OnContainerStopped(ContainerId containerId);

			/// <summary>
			/// The API is called when an exception is raised in the process of
			/// starting a container
			/// </summary>
			/// <param name="containerId">the Id of the container</param>
			/// <param name="t">the raised exception</param>
			void OnStartContainerError(ContainerId containerId, Exception t);

			/// <summary>
			/// The API is called when an exception is raised in the process of
			/// querying the status of a container
			/// </summary>
			/// <param name="containerId">the Id of the container</param>
			/// <param name="t">the raised exception</param>
			void OnGetContainerStatusError(ContainerId containerId, Exception t);

			/// <summary>
			/// The API is called when an exception is raised in the process of
			/// stopping a container
			/// </summary>
			/// <param name="containerId">the Id of the container</param>
			/// <param name="t">the raised exception</param>
			void OnStopContainerError(ContainerId containerId, Exception t);
		}
	}
}
