using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Client.Api.Async;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl
{
	public class AMRMClientAsyncImpl<T> : AMRMClientAsync<T>
		where T : AMRMClient.ContainerRequest
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl.AMRMClientAsyncImpl
			));

		private readonly AMRMClientAsyncImpl.HeartbeatThread heartbeatThread;

		private readonly AMRMClientAsyncImpl.CallbackHandlerThread handlerThread;

		private readonly BlockingQueue<AllocateResponse> responseQueue;

		private readonly object unregisterHeartbeatLock = new object();

		private volatile bool keepRunning;

		private volatile float progress;

		private volatile Exception savedException;

		public AMRMClientAsyncImpl(int intervalMs, AMRMClientAsync.CallbackHandler callbackHandler
			)
			: this(new AMRMClientImpl<T>(), intervalMs, callbackHandler)
		{
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public AMRMClientAsyncImpl(AMRMClient<T> client, int intervalMs, AMRMClientAsync.CallbackHandler
			 callbackHandler)
			: base(client, intervalMs, callbackHandler)
		{
			heartbeatThread = new AMRMClientAsyncImpl.HeartbeatThread(this);
			handlerThread = new AMRMClientAsyncImpl.CallbackHandlerThread(this);
			responseQueue = new LinkedBlockingQueue<AllocateResponse>();
			keepRunning = true;
			savedException = null;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			client.Init(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			handlerThread.SetDaemon(true);
			handlerThread.Start();
			client.Start();
			base.ServiceStart();
		}

		/// <summary>
		/// Tells the heartbeat and handler threads to stop and waits for them to
		/// terminate.
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			keepRunning = false;
			heartbeatThread.Interrupt();
			try
			{
				heartbeatThread.Join();
			}
			catch (Exception ex)
			{
				Log.Error("Error joining with heartbeat thread", ex);
			}
			client.Stop();
			handlerThread.Interrupt();
			base.ServiceStop();
		}

		public override void SetHeartbeatInterval(int interval)
		{
			heartbeatIntervalMs.Set(interval);
		}

		public override IList<ICollection<T>> GetMatchingRequests(Priority priority, string
			 resourceName, Resource capability)
		{
			return client.GetMatchingRequests(priority, resourceName, capability);
		}

		/// <summary>Registers this application master with the resource manager.</summary>
		/// <remarks>
		/// Registers this application master with the resource manager. On successful
		/// registration, starts the heartbeating thread.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override RegisterApplicationMasterResponse RegisterApplicationMaster(string
			 appHostName, int appHostPort, string appTrackingUrl)
		{
			RegisterApplicationMasterResponse response = client.RegisterApplicationMaster(appHostName
				, appHostPort, appTrackingUrl);
			heartbeatThread.Start();
			return response;
		}

		/// <summary>Unregister the application master.</summary>
		/// <remarks>Unregister the application master. This must be called in the end.</remarks>
		/// <param name="appStatus">Success/Failure status of the master</param>
		/// <param name="appMessage">Diagnostics message on failure</param>
		/// <param name="appTrackingUrl">New URL to get master info</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void UnregisterApplicationMaster(FinalApplicationStatus appStatus
			, string appMessage, string appTrackingUrl)
		{
			lock (unregisterHeartbeatLock)
			{
				keepRunning = false;
				client.UnregisterApplicationMaster(appStatus, appMessage, appTrackingUrl);
			}
		}

		/// <summary>Request containers for resources before calling <code>allocate</code></summary>
		/// <param name="req">Resource request</param>
		public override void AddContainerRequest(T req)
		{
			client.AddContainerRequest(req);
		}

		/// <summary>Remove previous container request.</summary>
		/// <remarks>
		/// Remove previous container request. The previous container request may have
		/// already been sent to the ResourceManager. So even after the remove request
		/// the app must be prepared to receive an allocation for the previous request
		/// even after the remove request
		/// </remarks>
		/// <param name="req">Resource request</param>
		public override void RemoveContainerRequest(T req)
		{
			client.RemoveContainerRequest(req);
		}

		/// <summary>Release containers assigned by the Resource Manager.</summary>
		/// <remarks>
		/// Release containers assigned by the Resource Manager. If the app cannot use
		/// the container or wants to give up the container then it can release them.
		/// The app needs to make new requests for the released resource capability if
		/// it still needs it. eg. it released non-local resources
		/// </remarks>
		/// <param name="containerId"/>
		public override void ReleaseAssignedContainer(ContainerId containerId)
		{
			client.ReleaseAssignedContainer(containerId);
		}

		/// <summary>Get the currently available resources in the cluster.</summary>
		/// <remarks>
		/// Get the currently available resources in the cluster.
		/// A valid value is available after a call to allocate has been made
		/// </remarks>
		/// <returns>Currently available resources</returns>
		public override Resource GetAvailableResources()
		{
			return client.GetAvailableResources();
		}

		/// <summary>Get the current number of nodes in the cluster.</summary>
		/// <remarks>
		/// Get the current number of nodes in the cluster.
		/// A valid values is available after a call to allocate has been made
		/// </remarks>
		/// <returns>Current number of nodes in the cluster</returns>
		public override int GetClusterNodeCount()
		{
			return client.GetClusterNodeCount();
		}

		/// <summary>Update application's blacklist with addition or removal resources.</summary>
		/// <param name="blacklistAdditions">
		/// list of resources which should be added to the
		/// application blacklist
		/// </param>
		/// <param name="blacklistRemovals">
		/// list of resources which should be removed from the
		/// application blacklist
		/// </param>
		public override void UpdateBlacklist(IList<string> blacklistAdditions, IList<string
			> blacklistRemovals)
		{
			client.UpdateBlacklist(blacklistAdditions, blacklistRemovals);
		}

		private class HeartbeatThread : Sharpen.Thread
		{
			public HeartbeatThread(AMRMClientAsyncImpl<T> _enclosing)
				: base("AMRM Heartbeater thread")
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				while (true)
				{
					AllocateResponse response = null;
					// synchronization ensures we don't send heartbeats after unregistering
					lock (this._enclosing.unregisterHeartbeatLock)
					{
						if (!this._enclosing.keepRunning)
						{
							return;
						}
						try
						{
							response = this._enclosing.client.Allocate(this._enclosing.progress);
						}
						catch (ApplicationAttemptNotFoundException)
						{
							this._enclosing.handler.OnShutdownRequest();
							AMRMClientAsyncImpl.Log.Info("Shutdown requested. Stopping callback.");
							return;
						}
						catch (Exception ex)
						{
							AMRMClientAsyncImpl.Log.Error("Exception on heartbeat", ex);
							this._enclosing.savedException = ex;
							// interrupt handler thread in case it waiting on the queue
							this._enclosing.handlerThread.Interrupt();
							return;
						}
						if (response != null)
						{
							while (true)
							{
								try
								{
									this._enclosing.responseQueue.Put(response);
									break;
								}
								catch (Exception ex)
								{
									AMRMClientAsyncImpl.Log.Debug("Interrupted while waiting to put on response queue"
										, ex);
								}
							}
						}
					}
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.heartbeatIntervalMs.Get());
					}
					catch (Exception ex)
					{
						AMRMClientAsyncImpl.Log.Debug("Heartbeater interrupted", ex);
					}
				}
			}

			private readonly AMRMClientAsyncImpl<T> _enclosing;
		}

		private class CallbackHandlerThread : Sharpen.Thread
		{
			public CallbackHandlerThread(AMRMClientAsyncImpl<T> _enclosing)
				: base("AMRM Callback Handler Thread")
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				while (true)
				{
					if (!this._enclosing.keepRunning)
					{
						return;
					}
					try
					{
						AllocateResponse response;
						if (this._enclosing.savedException != null)
						{
							AMRMClientAsyncImpl.Log.Error("Stopping callback due to: ", this._enclosing.savedException
								);
							this._enclosing.handler.OnError(this._enclosing.savedException);
							return;
						}
						try
						{
							response = this._enclosing.responseQueue.Take();
						}
						catch (Exception ex)
						{
							AMRMClientAsyncImpl.Log.Info("Interrupted while waiting for queue", ex);
							continue;
						}
						IList<NodeReport> updatedNodes = response.GetUpdatedNodes();
						if (!updatedNodes.IsEmpty())
						{
							this._enclosing.handler.OnNodesUpdated(updatedNodes);
						}
						IList<ContainerStatus> completed = response.GetCompletedContainersStatuses();
						if (!completed.IsEmpty())
						{
							this._enclosing.handler.OnContainersCompleted(completed);
						}
						IList<Container> allocated = response.GetAllocatedContainers();
						if (!allocated.IsEmpty())
						{
							this._enclosing.handler.OnContainersAllocated(allocated);
						}
						this._enclosing.progress = this._enclosing.handler.GetProgress();
					}
					catch (Exception ex)
					{
						this._enclosing.handler.OnError(ex);
						// re-throw exception to end the thread
						throw new YarnRuntimeException(ex);
					}
				}
			}

			private readonly AMRMClientAsyncImpl<T> _enclosing;
		}
	}
}
