using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	/// <summary>Registers/unregisters to RM and sends heartbeats to RM.</summary>
	public abstract class RMCommunicator : AbstractService, RMHeartbeatHandler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.RM.RMCommunicator
			));

		private int rmPollInterval;

		protected internal ApplicationId applicationId;

		private readonly AtomicBoolean stopped;

		protected internal Sharpen.Thread allocatorThread;

		protected internal EventHandler eventHandler;

		protected internal ApplicationMasterProtocol scheduler;

		private readonly ClientService clientService;

		private Resource maxContainerCapability;

		protected internal IDictionary<ApplicationAccessType, string> applicationACLs;

		private volatile long lastHeartbeatTime;

		private ConcurrentLinkedQueue<Runnable> heartbeatCallbacks;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private readonly AppContext context;

		private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job;

		protected internal volatile bool isSignalled = false;

		private volatile bool shouldUnregister = true;

		private bool isApplicationMasterRegistered = false;

		private EnumSet<YarnServiceProtos.SchedulerResourceTypes> schedulerResourceTypes;

		public RMCommunicator(ClientService clientService, AppContext context)
			: base("RMCommunicator")
		{
			//millis
			// Has a signal (SIGTERM etc) been issued?
			this.clientService = clientService;
			this.context = context;
			this.eventHandler = context.GetEventHandler();
			this.applicationId = context.GetApplicationID();
			this.stopped = new AtomicBoolean(false);
			this.heartbeatCallbacks = new ConcurrentLinkedQueue<Runnable>();
			this.schedulerResourceTypes = EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes
				.Memory);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			rmPollInterval = conf.GetInt(MRJobConfig.MrAmToRmHeartbeatIntervalMs, MRJobConfig
				.DefaultMrAmToRmHeartbeatIntervalMs);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			scheduler = CreateSchedulerProxy();
			JobID id = TypeConverter.FromYarn(this.applicationId);
			JobId jobId = TypeConverter.ToYarn(id);
			job = context.GetJob(jobId);
			Register();
			StartAllocatorThread();
			base.ServiceStart();
		}

		protected internal virtual AppContext GetContext()
		{
			return context;
		}

		protected internal virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob()
		{
			return job;
		}

		/// <summary>Get the appProgress.</summary>
		/// <remarks>Get the appProgress. Can be used only after this component is started.</remarks>
		/// <returns>the appProgress.</returns>
		protected internal virtual float GetApplicationProgress()
		{
			// For now just a single job. In future when we have a DAG, we need an
			// aggregate progress.
			return this.job.GetProgress();
		}

		protected internal virtual void Register()
		{
			//Register
			IPEndPoint serviceAddr = null;
			if (clientService != null)
			{
				serviceAddr = clientService.GetBindAddress();
			}
			try
			{
				RegisterApplicationMasterRequest request = recordFactory.NewRecordInstance<RegisterApplicationMasterRequest
					>();
				if (serviceAddr != null)
				{
					request.SetHost(serviceAddr.GetHostName());
					request.SetRpcPort(serviceAddr.Port);
					request.SetTrackingUrl(MRWebAppUtil.GetAMWebappScheme(GetConfig()) + serviceAddr.
						GetHostName() + ":" + clientService.GetHttpPort());
				}
				RegisterApplicationMasterResponse response = scheduler.RegisterApplicationMaster(
					request);
				isApplicationMasterRegistered = true;
				maxContainerCapability = response.GetMaximumResourceCapability();
				this.context.GetClusterInfo().SetMaxContainerCapability(maxContainerCapability);
				if (UserGroupInformation.IsSecurityEnabled())
				{
					SetClientToAMToken(response.GetClientToAMTokenMasterKey());
				}
				this.applicationACLs = response.GetApplicationACLs();
				Log.Info("maxContainerCapability: " + maxContainerCapability);
				string queue = response.GetQueue();
				Log.Info("queue: " + queue);
				job.SetQueueName(queue);
				Sharpen.Collections.AddAll(this.schedulerResourceTypes, response.GetSchedulerResourceTypes
					());
			}
			catch (Exception are)
			{
				Log.Error("Exception while registering", are);
				throw new YarnRuntimeException(are);
			}
		}

		private void SetClientToAMToken(ByteBuffer clientToAMTokenMasterKey)
		{
			byte[] key = ((byte[])clientToAMTokenMasterKey.Array());
			context.GetClientToAMTokenSecretManager().SetMasterKey(key);
		}

		protected internal virtual void Unregister()
		{
			try
			{
				DoUnregistration();
			}
			catch (Exception are)
			{
				Log.Error("Exception while unregistering ", are);
				// if unregistration failed, isLastAMRetry needs to be recalculated
				// to see whether AM really has the chance to retry
				MRAppMaster.RunningAppContext raContext = (MRAppMaster.RunningAppContext)context;
				raContext.ResetIsLastAMRetry();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		protected internal virtual void DoUnregistration()
		{
			FinalApplicationStatus finishState = FinalApplicationStatus.Undefined;
			JobImpl jobImpl = (JobImpl)job;
			if (jobImpl.GetInternalState() == JobStateInternal.Succeeded)
			{
				finishState = FinalApplicationStatus.Succeeded;
			}
			else
			{
				if (jobImpl.GetInternalState() == JobStateInternal.Killed || (jobImpl.GetInternalState
					() == JobStateInternal.Running && isSignalled))
				{
					finishState = FinalApplicationStatus.Killed;
				}
				else
				{
					if (jobImpl.GetInternalState() == JobStateInternal.Failed || jobImpl.GetInternalState
						() == JobStateInternal.Error)
					{
						finishState = FinalApplicationStatus.Failed;
					}
				}
			}
			StringBuilder sb = new StringBuilder();
			foreach (string s in job.GetDiagnostics())
			{
				sb.Append(s).Append("\n");
			}
			Log.Info("Setting job diagnostics to " + sb.ToString());
			string historyUrl = MRWebAppUtil.GetApplicationWebURLOnJHSWithScheme(GetConfig(), 
				context.GetApplicationID());
			Log.Info("History url is " + historyUrl);
			FinishApplicationMasterRequest request = FinishApplicationMasterRequest.NewInstance
				(finishState, sb.ToString(), historyUrl);
			try
			{
				while (true)
				{
					FinishApplicationMasterResponse response = scheduler.FinishApplicationMaster(request
						);
					if (response.GetIsUnregistered())
					{
						// When excepting ClientService, other services are already stopped,
						// it is safe to let clients know the final states. ClientService
						// should wait for some time so clients have enough time to know the
						// final states.
						MRAppMaster.RunningAppContext raContext = (MRAppMaster.RunningAppContext)context;
						raContext.MarkSuccessfulUnregistration();
						break;
					}
					Log.Info("Waiting for application to be successfully unregistered.");
					Sharpen.Thread.Sleep(rmPollInterval);
				}
			}
			catch (ApplicationMasterNotRegisteredException)
			{
				// RM might have restarted or failed over and so lost the fact that AM had
				// registered before.
				Register();
				DoUnregistration();
			}
		}

		protected internal virtual Resource GetMaxContainerCapability()
		{
			return maxContainerCapability;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (stopped.GetAndSet(true))
			{
				// return if already stopped
				return;
			}
			if (allocatorThread != null)
			{
				allocatorThread.Interrupt();
				try
				{
					allocatorThread.Join();
				}
				catch (Exception ie)
				{
					Log.Warn("InterruptedException while stopping", ie);
				}
			}
			if (isApplicationMasterRegistered && shouldUnregister)
			{
				Unregister();
			}
			base.ServiceStop();
		}

		public class AllocatorRunnable : Runnable
		{
			public virtual void Run()
			{
				while (!this._enclosing.stopped.Get() && !Sharpen.Thread.CurrentThread().IsInterrupted
					())
				{
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.rmPollInterval);
						try
						{
							this._enclosing.Heartbeat();
						}
						catch (RMContainerAllocationException e)
						{
							RMCommunicator.Log.Error("Error communicating with RM: " + e.Message, e);
							return;
						}
						catch (Exception e)
						{
							RMCommunicator.Log.Error("ERROR IN CONTACTING RM. ", e);
							continue;
						}
						// TODO: for other exceptions
						this._enclosing.lastHeartbeatTime = this._enclosing.context.GetClock().GetTime();
						this._enclosing.ExecuteHeartbeatCallbacks();
					}
					catch (Exception)
					{
						if (!this._enclosing.stopped.Get())
						{
							RMCommunicator.Log.Warn("Allocated thread interrupted. Returning.");
						}
						return;
					}
				}
			}

			internal AllocatorRunnable(RMCommunicator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMCommunicator _enclosing;
		}

		protected internal virtual void StartAllocatorThread()
		{
			allocatorThread = new Sharpen.Thread(new RMCommunicator.AllocatorRunnable(this));
			allocatorThread.SetName("RMCommunicator Allocator");
			allocatorThread.Start();
		}

		protected internal virtual ApplicationMasterProtocol CreateSchedulerProxy()
		{
			Configuration conf = GetConfig();
			try
			{
				return ClientRMProxy.CreateRMProxy<ApplicationMasterProtocol>(conf);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal abstract void Heartbeat();

		private void ExecuteHeartbeatCallbacks()
		{
			Runnable callback = null;
			while ((callback = heartbeatCallbacks.Poll()) != null)
			{
				callback.Run();
			}
		}

		public virtual long GetLastHeartbeatTime()
		{
			return lastHeartbeatTime;
		}

		public virtual void RunOnNextHeartbeat(Runnable callback)
		{
			heartbeatCallbacks.AddItem(callback);
		}

		public virtual void SetShouldUnregister(bool shouldUnregister)
		{
			this.shouldUnregister = shouldUnregister;
			Log.Info("RMCommunicator notified that shouldUnregistered is: " + shouldUnregister
				);
		}

		public virtual void SetSignalled(bool isSignalled)
		{
			this.isSignalled = isSignalled;
			Log.Info("RMCommunicator notified that isSignalled is: " + isSignalled);
		}

		[VisibleForTesting]
		protected internal virtual bool IsApplicationMasterRegistered()
		{
			return isApplicationMasterRegistered;
		}

		public virtual EnumSet<YarnServiceProtos.SchedulerResourceTypes> GetSchedulerResourceTypes
			()
		{
			return schedulerResourceTypes;
		}
	}
}
