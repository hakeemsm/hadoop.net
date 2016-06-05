using System.Collections.Generic;
using System.Text;
using System.Threading;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.State;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	/// <summary>Datum representing a localized resource.</summary>
	/// <remarks>
	/// Datum representing a localized resource. Holds the statemachine of a
	/// resource. State of the resource is one of
	/// <see cref="ResourceState"/>
	/// .
	/// </remarks>
	public class LocalizedResource : EventHandler<ResourceEvent>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalizedResource
			));

		internal volatile Path localPath;

		internal volatile long size = -1;

		internal readonly LocalResourceRequest rsrc;

		internal readonly Dispatcher dispatcher;

		internal readonly StateMachine<ResourceState, ResourceEventType, ResourceEvent> stateMachine;

		internal readonly Semaphore sem = Sharpen.Extensions.CreateSemaphore(1);

		internal readonly Queue<ContainerId> @ref;

		private readonly Lock readLock;

		private readonly Lock writeLock;

		internal readonly AtomicLong timestamp;

		private static readonly StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalizedResource
			, ResourceState, ResourceEventType, ResourceEvent> stateMachineFactory = new StateMachineFactory
			<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalizedResource
			, ResourceState, ResourceEventType, ResourceEvent>(ResourceState.Init).AddTransition
			(ResourceState.Init, ResourceState.Downloading, ResourceEventType.Request, new LocalizedResource.FetchResourceTransition
			()).AddTransition(ResourceState.Init, ResourceState.Localized, ResourceEventType
			.Recovered, new LocalizedResource.RecoveredTransition()).AddTransition(ResourceState
			.Downloading, ResourceState.Downloading, ResourceEventType.Request, new LocalizedResource.FetchResourceTransition
			()).AddTransition(ResourceState.Downloading, ResourceState.Localized, ResourceEventType
			.Localized, new LocalizedResource.FetchSuccessTransition()).AddTransition(ResourceState
			.Downloading, ResourceState.Downloading, ResourceEventType.Release, new LocalizedResource.ReleaseTransition
			()).AddTransition(ResourceState.Downloading, ResourceState.Failed, ResourceEventType
			.LocalizationFailed, new LocalizedResource.FetchFailedTransition()).AddTransition
			(ResourceState.Localized, ResourceState.Localized, ResourceEventType.Request, new 
			LocalizedResource.LocalizedResourceTransition()).AddTransition(ResourceState.Localized
			, ResourceState.Localized, ResourceEventType.Release, new LocalizedResource.ReleaseTransition
			()).InstallTopology();

		public LocalizedResource(LocalResourceRequest rsrc, Dispatcher dispatcher)
		{
			timestamp = new AtomicLong(CurrentTime());
			// Queue of containers using this localized
			// resource
			// From INIT (ref == 0, awaiting req)
			// From DOWNLOADING (ref > 0, may be localizing)
			// TODO: Duplicate addition!!
			// From LOCALIZED (ref >= 0, on disk)
			this.rsrc = rsrc;
			this.dispatcher = dispatcher;
			this.@ref = new List<ContainerId>();
			ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
			this.readLock = readWriteLock.ReadLock();
			this.writeLock = readWriteLock.WriteLock();
			this.stateMachine = stateMachineFactory.Make(this);
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("{ ").Append(rsrc.ToString()).Append(",").Append(GetState() == ResourceState
				.Localized ? GetLocalPath() + "," + GetSize() : "pending").Append(",[");
			try
			{
				this.readLock.Lock();
				foreach (ContainerId c in @ref)
				{
					sb.Append("(").Append(c.ToString()).Append(")");
				}
				sb.Append("],").Append(GetTimestamp()).Append(",").Append(GetState()).Append("}");
				return sb.ToString();
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private void Release(ContainerId container)
		{
			if (@ref.Remove(container))
			{
				// updating the timestamp only in case of success.
				timestamp.Set(CurrentTime());
			}
			else
			{
				Log.Info("Container " + container + " doesn't exist in the container list of the Resource "
					 + this + " to which it sent RELEASE event");
			}
		}

		private long CurrentTime()
		{
			return Runtime.NanoTime();
		}

		public virtual ResourceState GetState()
		{
			this.readLock.Lock();
			try
			{
				return stateMachine.GetCurrentState();
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual LocalResourceRequest GetRequest()
		{
			return rsrc;
		}

		public virtual Path GetLocalPath()
		{
			return localPath;
		}

		public virtual void SetLocalPath(Path localPath)
		{
			this.localPath = Path.GetPathWithoutSchemeAndAuthority(localPath);
		}

		public virtual long GetTimestamp()
		{
			return timestamp.Get();
		}

		public virtual long GetSize()
		{
			return size;
		}

		public virtual int GetRefCount()
		{
			return @ref.Count;
		}

		public virtual bool TryAcquire()
		{
			return sem.TryAcquire();
		}

		public virtual void Unlock()
		{
			sem.Release();
		}

		public virtual void Handle(ResourceEvent @event)
		{
			try
			{
				this.writeLock.Lock();
				Path resourcePath = @event.GetLocalResourceRequest().GetPath();
				Log.Debug("Processing " + resourcePath + " of type " + @event.GetType());
				ResourceState oldState = this.stateMachine.GetCurrentState();
				ResourceState newState = null;
				try
				{
					newState = this.stateMachine.DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Warn("Can't handle this event at current state", e);
				}
				if (oldState != newState)
				{
					Log.Info("Resource " + resourcePath + (localPath != null ? "(->" + localPath + ")"
						 : string.Empty) + " transitioned from " + oldState + " to " + newState);
				}
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		internal abstract class ResourceTransition : SingleArcTransition<LocalizedResource
			, ResourceEvent>
		{
			public abstract void Transition(LocalizedResource arg1, ResourceEvent arg2);
			// typedef
		}

		/// <summary>Transition from INIT to DOWNLOADING.</summary>
		/// <remarks>
		/// Transition from INIT to DOWNLOADING.
		/// Sends a
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event.LocalizerResourceRequestEvent
		/// 	"/>
		/// to the
		/// <see cref="ResourceLocalizationService"/>
		/// .
		/// </remarks>
		private class FetchResourceTransition : LocalizedResource.ResourceTransition
		{
			// dispatcher not typed
			public override void Transition(LocalizedResource rsrc, ResourceEvent @event)
			{
				ResourceRequestEvent req = (ResourceRequestEvent)@event;
				LocalizerContext ctxt = req.GetContext();
				ContainerId container = ctxt.GetContainerId();
				rsrc.@ref.AddItem(container);
				rsrc.dispatcher.GetEventHandler().Handle(new LocalizerResourceRequestEvent(rsrc, 
					req.GetVisibility(), ctxt, req.GetLocalResourceRequest().GetPattern()));
			}
		}

		/// <summary>Resource localized, notify waiting containers.</summary>
		private class FetchSuccessTransition : LocalizedResource.ResourceTransition
		{
			// dispatcher not typed
			public override void Transition(LocalizedResource rsrc, ResourceEvent @event)
			{
				ResourceLocalizedEvent locEvent = (ResourceLocalizedEvent)@event;
				rsrc.localPath = Path.GetPathWithoutSchemeAndAuthority(locEvent.GetLocation());
				rsrc.size = locEvent.GetSize();
				foreach (ContainerId container in rsrc.@ref)
				{
					rsrc.dispatcher.GetEventHandler().Handle(new ContainerResourceLocalizedEvent(container
						, rsrc.rsrc, rsrc.localPath));
				}
			}
		}

		/// <summary>Resource localization failed, notify waiting containers.</summary>
		private class FetchFailedTransition : LocalizedResource.ResourceTransition
		{
			public override void Transition(LocalizedResource rsrc, ResourceEvent @event)
			{
				ResourceFailedLocalizationEvent failedEvent = (ResourceFailedLocalizationEvent)@event;
				Queue<ContainerId> containers = rsrc.@ref;
				foreach (ContainerId container in containers)
				{
					rsrc.dispatcher.GetEventHandler().Handle(new ContainerResourceFailedEvent(container
						, failedEvent.GetLocalResourceRequest(), failedEvent.GetDiagnosticMessage()));
				}
			}
		}

		/// <summary>Resource already localized, notify immediately.</summary>
		private class LocalizedResourceTransition : LocalizedResource.ResourceTransition
		{
			// dispatcher not typed
			public override void Transition(LocalizedResource rsrc, ResourceEvent @event)
			{
				// notify waiting containers
				ResourceRequestEvent reqEvent = (ResourceRequestEvent)@event;
				ContainerId container = reqEvent.GetContext().GetContainerId();
				rsrc.@ref.AddItem(container);
				rsrc.dispatcher.GetEventHandler().Handle(new ContainerResourceLocalizedEvent(container
					, rsrc.rsrc, rsrc.localPath));
			}
		}

		/// <summary>Decrement resource count, update timestamp.</summary>
		private class ReleaseTransition : LocalizedResource.ResourceTransition
		{
			public override void Transition(LocalizedResource rsrc, ResourceEvent @event)
			{
				// Note: assumes that localizing container must succeed or fail
				ResourceReleaseEvent relEvent = (ResourceReleaseEvent)@event;
				rsrc.Release(relEvent.GetContainer());
			}
		}

		private class RecoveredTransition : LocalizedResource.ResourceTransition
		{
			public override void Transition(LocalizedResource rsrc, ResourceEvent @event)
			{
				ResourceRecoveredEvent recoveredEvent = (ResourceRecoveredEvent)@event;
				rsrc.localPath = recoveredEvent.GetLocalPath();
				rsrc.size = recoveredEvent.GetSize();
			}
		}
	}
}
