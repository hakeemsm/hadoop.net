using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>This class keeps track of tasks that have already been launched.</summary>
	/// <remarks>
	/// This class keeps track of tasks that have already been launched. It
	/// determines if a task is alive and running or marks a task as dead if it does
	/// not hear from it for a long time.
	/// </remarks>
	public class TaskHeartbeatHandler : AbstractService
	{
		private class ReportTime
		{
			private long lastProgress;

			public ReportTime(long time)
			{
				SetLastProgress(time);
			}

			public virtual void SetLastProgress(long time)
			{
				lock (this)
				{
					lastProgress = time;
				}
			}

			public virtual long GetLastProgress()
			{
				lock (this)
				{
					return lastProgress;
				}
			}
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TaskHeartbeatHandler));

		private Sharpen.Thread lostTaskCheckerThread;

		private volatile bool stopped;

		private int taskTimeOut = 5 * 60 * 1000;

		private int taskTimeOutCheckInterval = 30 * 1000;

		private readonly EventHandler eventHandler;

		private readonly Clock clock;

		private ConcurrentMap<TaskAttemptId, TaskHeartbeatHandler.ReportTime> runningAttempts;

		public TaskHeartbeatHandler(EventHandler eventHandler, Clock clock, int numThreads
			)
			: base("TaskHeartbeatHandler")
		{
			//thread which runs periodically to see the last time since a heartbeat is
			//received from a task.
			// 5 mins
			// 30 seconds.
			this.eventHandler = eventHandler;
			this.clock = clock;
			runningAttempts = new ConcurrentHashMap<TaskAttemptId, TaskHeartbeatHandler.ReportTime
				>(16, 0.75f, numThreads);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			taskTimeOut = conf.GetInt(MRJobConfig.TaskTimeout, 5 * 60 * 1000);
			taskTimeOutCheckInterval = conf.GetInt(MRJobConfig.TaskTimeoutCheckIntervalMs, 30
				 * 1000);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			lostTaskCheckerThread = new Sharpen.Thread(new TaskHeartbeatHandler.PingChecker(this
				));
			lostTaskCheckerThread.SetName("TaskHeartbeatHandler PingChecker");
			lostTaskCheckerThread.Start();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			stopped = true;
			if (lostTaskCheckerThread != null)
			{
				lostTaskCheckerThread.Interrupt();
			}
			base.ServiceStop();
		}

		public virtual void Progressing(TaskAttemptId attemptID)
		{
			//only put for the registered attempts
			//TODO throw an exception if the task isn't registered.
			TaskHeartbeatHandler.ReportTime time = runningAttempts[attemptID];
			if (time != null)
			{
				time.SetLastProgress(clock.GetTime());
			}
		}

		public virtual void Register(TaskAttemptId attemptID)
		{
			runningAttempts[attemptID] = new TaskHeartbeatHandler.ReportTime(clock.GetTime());
		}

		public virtual void Unregister(TaskAttemptId attemptID)
		{
			Sharpen.Collections.Remove(runningAttempts, attemptID);
		}

		private class PingChecker : Runnable
		{
			public virtual void Run()
			{
				while (!this._enclosing.stopped && !Sharpen.Thread.CurrentThread().IsInterrupted(
					))
				{
					IEnumerator<KeyValuePair<TaskAttemptId, TaskHeartbeatHandler.ReportTime>> iterator
						 = this._enclosing.runningAttempts.GetEnumerator();
					// avoid calculating current time everytime in loop
					long currentTime = this._enclosing.clock.GetTime();
					while (iterator.HasNext())
					{
						KeyValuePair<TaskAttemptId, TaskHeartbeatHandler.ReportTime> entry = iterator.Next
							();
						bool taskTimedOut = (this._enclosing.taskTimeOut > 0) && (currentTime > (entry.Value
							.GetLastProgress() + this._enclosing.taskTimeOut));
						if (taskTimedOut)
						{
							// task is lost, remove from the list and raise lost event
							iterator.Remove();
							this._enclosing.eventHandler.Handle(new TaskAttemptDiagnosticsUpdateEvent(entry.Key
								, "AttemptID:" + entry.Key.ToString() + " Timed out after " + this._enclosing.taskTimeOut
								 / 1000 + " secs"));
							this._enclosing.eventHandler.Handle(new TaskAttemptEvent(entry.Key, TaskAttemptEventType
								.TaTimedOut));
						}
					}
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.taskTimeOutCheckInterval);
					}
					catch (Exception)
					{
						TaskHeartbeatHandler.Log.Info("TaskHeartbeatHandler thread interrupted");
						break;
					}
				}
			}

			internal PingChecker(TaskHeartbeatHandler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TaskHeartbeatHandler _enclosing;
		}
	}
}
