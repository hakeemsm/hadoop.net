using System;
using System.IO;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Commit
{
	public class CommitterEventHandler : AbstractService, EventHandler<CommitterEvent
		>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Commit.CommitterEventHandler
			));

		private readonly AppContext context;

		private readonly OutputCommitter committer;

		private readonly RMHeartbeatHandler rmHeartbeatHandler;

		private ThreadPoolExecutor launcherPool;

		private Sharpen.Thread eventHandlingThread;

		private BlockingQueue<CommitterEvent> eventQueue = new LinkedBlockingQueue<CommitterEvent
			>();

		private readonly AtomicBoolean stopped;

		private readonly ClassLoader jobClassLoader;

		private Sharpen.Thread jobCommitThread = null;

		private int commitThreadCancelTimeoutMs;

		private long commitWindowMs;

		private FileSystem fs;

		private Path startCommitFile;

		private Path endCommitSuccessFile;

		private Path endCommitFailureFile;

		public CommitterEventHandler(AppContext context, OutputCommitter committer, RMHeartbeatHandler
			 rmHeartbeatHandler)
			: this(context, committer, rmHeartbeatHandler, null)
		{
		}

		public CommitterEventHandler(AppContext context, OutputCommitter committer, RMHeartbeatHandler
			 rmHeartbeatHandler, ClassLoader jobClassLoader)
			: base("CommitterEventHandler")
		{
			this.context = context;
			this.committer = committer;
			this.rmHeartbeatHandler = rmHeartbeatHandler;
			this.stopped = new AtomicBoolean(false);
			this.jobClassLoader = jobClassLoader;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			commitThreadCancelTimeoutMs = conf.GetInt(MRJobConfig.MrAmCommitterCancelTimeoutMs
				, MRJobConfig.DefaultMrAmCommitterCancelTimeoutMs);
			commitWindowMs = conf.GetLong(MRJobConfig.MrAmCommitWindowMs, MRJobConfig.DefaultMrAmCommitWindowMs
				);
			try
			{
				fs = FileSystem.Get(conf);
				JobID id = TypeConverter.FromYarn(context.GetApplicationID());
				JobId jobId = TypeConverter.ToYarn(id);
				string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
				startCommitFile = MRApps.GetStartJobCommitFile(conf, user, jobId);
				endCommitSuccessFile = MRApps.GetEndJobCommitSuccessFile(conf, user, jobId);
				endCommitFailureFile = MRApps.GetEndJobCommitFailureFile(conf, user, jobId);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			ThreadFactoryBuilder tfBuilder = new ThreadFactoryBuilder().SetNameFormat("CommitterEvent Processor #%d"
				);
			if (jobClassLoader != null)
			{
				// if the job classloader is enabled, we need to use the job classloader
				// as the thread context classloader (TCCL) of these threads in case the
				// committer needs to load another class via TCCL
				ThreadFactory backingTf = new _ThreadFactory_125(this);
				tfBuilder.SetThreadFactory(backingTf);
			}
			ThreadFactory tf = tfBuilder.Build();
			launcherPool = new ThreadPoolExecutor(5, 5, 1, TimeUnit.Hours, new LinkedBlockingQueue
				<Runnable>(), tf);
			eventHandlingThread = new Sharpen.Thread(new _Runnable_138(this));
			// the events from the queue are handled in parallel
			// using a thread pool
			eventHandlingThread.SetName("CommitterEvent Handler");
			eventHandlingThread.Start();
			base.ServiceStart();
		}

		private sealed class _ThreadFactory_125 : ThreadFactory
		{
			public _ThreadFactory_125(CommitterEventHandler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public Sharpen.Thread NewThread(Runnable r)
			{
				Sharpen.Thread thread = new Sharpen.Thread(r);
				thread.SetContextClassLoader(this._enclosing.jobClassLoader);
				return thread;
			}

			private readonly CommitterEventHandler _enclosing;
		}

		private sealed class _Runnable_138 : Runnable
		{
			public _Runnable_138(CommitterEventHandler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				CommitterEvent @event = null;
				while (!this._enclosing.stopped.Get() && !Sharpen.Thread.CurrentThread().IsInterrupted
					())
				{
					try
					{
						@event = this._enclosing.eventQueue.Take();
					}
					catch (Exception e)
					{
						if (!this._enclosing.stopped.Get())
						{
							Org.Apache.Hadoop.Mapreduce.V2.App.Commit.CommitterEventHandler.Log.Error("Returning, interrupted : "
								 + e);
						}
						return;
					}
					this._enclosing.launcherPool.Execute(new CommitterEventHandler.EventProcessor(this
						, @event));
				}
			}

			private readonly CommitterEventHandler _enclosing;
		}

		public virtual void Handle(CommitterEvent @event)
		{
			try
			{
				eventQueue.Put(@event);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (stopped.GetAndSet(true))
			{
				// return if already stopped
				return;
			}
			if (eventHandlingThread != null)
			{
				eventHandlingThread.Interrupt();
			}
			if (launcherPool != null)
			{
				launcherPool.Shutdown();
			}
			base.ServiceStop();
		}

		/// <exception cref="System.IO.IOException"/>
		private void JobCommitStarted()
		{
			lock (this)
			{
				if (jobCommitThread != null)
				{
					throw new IOException("Commit while another commit thread active: " + jobCommitThread
						.ToString());
				}
				jobCommitThread = Sharpen.Thread.CurrentThread();
			}
		}

		private void JobCommitEnded()
		{
			lock (this)
			{
				if (jobCommitThread == Sharpen.Thread.CurrentThread())
				{
					jobCommitThread = null;
					Sharpen.Runtime.NotifyAll(this);
				}
			}
		}

		private void CancelJobCommit()
		{
			lock (this)
			{
				Sharpen.Thread threadCommitting = jobCommitThread;
				if (threadCommitting != null && threadCommitting.IsAlive())
				{
					Log.Info("Cancelling commit");
					threadCommitting.Interrupt();
					// wait up to configured timeout for commit thread to finish
					long now = context.GetClock().GetTime();
					long timeoutTimestamp = now + commitThreadCancelTimeoutMs;
					try
					{
						while (jobCommitThread == threadCommitting && now > timeoutTimestamp)
						{
							Sharpen.Runtime.Wait(this, now - timeoutTimestamp);
							now = context.GetClock().GetTime();
						}
					}
					catch (Exception)
					{
					}
				}
			}
		}

		private class EventProcessor : Runnable
		{
			private CommitterEvent @event;

			internal EventProcessor(CommitterEventHandler _enclosing, CommitterEvent @event)
			{
				this._enclosing = _enclosing;
				this.@event = @event;
			}

			public virtual void Run()
			{
				CommitterEventHandler.Log.Info("Processing the event " + this.@event.ToString());
				switch (this.@event.GetType())
				{
					case CommitterEventType.JobSetup:
					{
						this.HandleJobSetup((CommitterJobSetupEvent)this.@event);
						break;
					}

					case CommitterEventType.JobCommit:
					{
						this.HandleJobCommit((CommitterJobCommitEvent)this.@event);
						break;
					}

					case CommitterEventType.JobAbort:
					{
						this.HandleJobAbort((CommitterJobAbortEvent)this.@event);
						break;
					}

					case CommitterEventType.TaskAbort:
					{
						this.HandleTaskAbort((CommitterTaskAbortEvent)this.@event);
						break;
					}

					default:
					{
						throw new YarnRuntimeException("Unexpected committer event " + this.@event.ToString
							());
					}
				}
			}

			protected internal virtual void HandleJobSetup(CommitterJobSetupEvent @event)
			{
				try
				{
					this._enclosing.committer.SetupJob(@event.GetJobContext());
					this._enclosing.context.GetEventHandler().Handle(new JobSetupCompletedEvent(@event
						.GetJobID()));
				}
				catch (Exception e)
				{
					CommitterEventHandler.Log.Warn("Job setup failed", e);
					this._enclosing.context.GetEventHandler().Handle(new JobSetupFailedEvent(@event.GetJobID
						(), StringUtils.StringifyException(e)));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void Touchz(Path p)
			{
				this._enclosing.fs.Create(p, false).Close();
			}

			protected internal virtual void HandleJobCommit(CommitterJobCommitEvent @event)
			{
				try
				{
					this.Touchz(this._enclosing.startCommitFile);
					this._enclosing.JobCommitStarted();
					this.WaitForValidCommitWindow();
					this._enclosing.committer.CommitJob(@event.GetJobContext());
					this.Touchz(this._enclosing.endCommitSuccessFile);
					this._enclosing.context.GetEventHandler().Handle(new JobCommitCompletedEvent(@event
						.GetJobID()));
				}
				catch (Exception e)
				{
					try
					{
						this.Touchz(this._enclosing.endCommitFailureFile);
					}
					catch (Exception e2)
					{
						CommitterEventHandler.Log.Error("could not create failure file.", e2);
					}
					CommitterEventHandler.Log.Error("Could not commit job", e);
					this._enclosing.context.GetEventHandler().Handle(new JobCommitFailedEvent(@event.
						GetJobID(), StringUtils.StringifyException(e)));
				}
				finally
				{
					this._enclosing.JobCommitEnded();
				}
			}

			protected internal virtual void HandleJobAbort(CommitterJobAbortEvent @event)
			{
				this._enclosing.CancelJobCommit();
				try
				{
					this._enclosing.committer.AbortJob(@event.GetJobContext(), @event.GetFinalState()
						);
				}
				catch (Exception e)
				{
					CommitterEventHandler.Log.Warn("Could not abort job", e);
				}
				this._enclosing.context.GetEventHandler().Handle(new JobAbortCompletedEvent(@event
					.GetJobID(), @event.GetFinalState()));
			}

			protected internal virtual void HandleTaskAbort(CommitterTaskAbortEvent @event)
			{
				try
				{
					this._enclosing.committer.AbortTask(@event.GetAttemptContext());
				}
				catch (Exception e)
				{
					CommitterEventHandler.Log.Warn("Task cleanup failed for attempt " + @event.GetAttemptID
						(), e);
				}
				this._enclosing.context.GetEventHandler().Handle(new TaskAttemptEvent(@event.GetAttemptID
					(), TaskAttemptEventType.TaCleanupDone));
			}

			/// <exception cref="System.Exception"/>
			private void WaitForValidCommitWindow()
			{
				lock (this)
				{
					long lastHeartbeatTime = this._enclosing.rmHeartbeatHandler.GetLastHeartbeatTime(
						);
					long now = this._enclosing.context.GetClock().GetTime();
					while (now - lastHeartbeatTime > this._enclosing.commitWindowMs)
					{
						this._enclosing.rmHeartbeatHandler.RunOnNextHeartbeat(new _Runnable_325(this));
						Sharpen.Runtime.Wait(this);
						lastHeartbeatTime = this._enclosing.rmHeartbeatHandler.GetLastHeartbeatTime();
						now = this._enclosing.context.GetClock().GetTime();
					}
				}
			}

			private sealed class _Runnable_325 : Runnable
			{
				public _Runnable_325(EventProcessor _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void Run()
				{
					lock (this._enclosing)
					{
						Sharpen.Runtime.Notify(this._enclosing);
					}
				}

				private readonly EventProcessor _enclosing;
			}

			private readonly CommitterEventHandler _enclosing;
		}
	}
}
