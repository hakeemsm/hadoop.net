using System;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor
{
	public class SchedulingMonitor : AbstractService
	{
		private readonly SchedulingEditPolicy scheduleEditPolicy;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor.SchedulingMonitor
			));

		private Sharpen.Thread checkerThread;

		private volatile bool stopped;

		private long monitorInterval;

		private RMContext rmContext;

		public SchedulingMonitor(RMContext rmContext, SchedulingEditPolicy scheduleEditPolicy
			)
			: base("SchedulingMonitor (" + scheduleEditPolicy.GetPolicyName() + ")")
		{
			//thread which runs periodically to see the last time since a heartbeat is
			//received.
			this.scheduleEditPolicy = scheduleEditPolicy;
			this.rmContext = rmContext;
		}

		public virtual long GetMonitorInterval()
		{
			return monitorInterval;
		}

		[VisibleForTesting]
		public virtual SchedulingEditPolicy GetSchedulingEditPolicy()
		{
			lock (this)
			{
				return scheduleEditPolicy;
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			scheduleEditPolicy.Init(conf, rmContext, (PreemptableResourceScheduler)rmContext.
				GetScheduler());
			this.monitorInterval = scheduleEditPolicy.GetMonitoringInterval();
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			System.Diagnostics.Debug.Assert(!stopped, "starting when already stopped");
			checkerThread = new Sharpen.Thread(new SchedulingMonitor.PreemptionChecker(this));
			checkerThread.SetName(GetName());
			checkerThread.Start();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			stopped = true;
			if (checkerThread != null)
			{
				checkerThread.Interrupt();
			}
			base.ServiceStop();
		}

		[VisibleForTesting]
		public virtual void InvokePolicy()
		{
			scheduleEditPolicy.EditSchedule();
		}

		private class PreemptionChecker : Runnable
		{
			public virtual void Run()
			{
				while (!this._enclosing.stopped && !Sharpen.Thread.CurrentThread().IsInterrupted(
					))
				{
					//invoke the preemption policy at a regular pace
					//the policy will generate preemption or kill events
					//managed by the dispatcher
					this._enclosing.InvokePolicy();
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.monitorInterval);
					}
					catch (Exception)
					{
						SchedulingMonitor.Log.Info(this._enclosing.GetName() + " thread interrupted");
						break;
					}
				}
			}

			internal PreemptionChecker(SchedulingMonitor _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly SchedulingMonitor _enclosing;
		}
	}
}
