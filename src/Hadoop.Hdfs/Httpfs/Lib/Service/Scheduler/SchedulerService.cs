using System;
using Org.Apache.Hadoop.Lib.Lang;
using Org.Apache.Hadoop.Lib.Server;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Util;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Scheduler
{
	public class SchedulerService : BaseService, Org.Apache.Hadoop.Lib.Service.Scheduler
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Lib.Service.Scheduler.SchedulerService
			));

		private const string InstGroup = "scheduler";

		public const string Prefix = "scheduler";

		public const string ConfThreads = "threads";

		private ScheduledExecutorService scheduler;

		public SchedulerService()
			: base(Prefix)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		protected internal override void Init()
		{
			int threads = GetServiceConfig().GetInt(ConfThreads, 5);
			scheduler = new ScheduledThreadPoolExecutor(threads);
			Log.Debug("Scheduler started");
		}

		public override void Destroy()
		{
			try
			{
				long limit = Time.Now() + 30 * 1000;
				scheduler.ShutdownNow();
				while (!scheduler.AwaitTermination(1000, TimeUnit.Milliseconds))
				{
					Log.Debug("Waiting for scheduler to shutdown");
					if (Time.Now() > limit)
					{
						Log.Warn("Gave up waiting for scheduler to shutdown");
						break;
					}
				}
				if (scheduler.IsTerminated())
				{
					Log.Debug("Scheduler shutdown");
				}
			}
			catch (Exception ex)
			{
				Log.Warn(ex.Message, ex);
			}
		}

		public override Type[] GetServiceDependencies()
		{
			return new Type[] { typeof(Instrumentation) };
		}

		public override Type GetInterface()
		{
			return typeof(Org.Apache.Hadoop.Lib.Service.Scheduler);
		}

		public virtual void Schedule<_T0>(Callable<_T0> callable, long delay, long interval
			, TimeUnit unit)
		{
			Check.NotNull(callable, "callable");
			if (!scheduler.IsShutdown())
			{
				Log.Debug("Scheduling callable [{}], interval [{}] seconds, delay [{}] in [{}]", 
					new object[] { callable, delay, interval, unit });
				Runnable r = new _Runnable_98(this, callable);
				scheduler.ScheduleWithFixedDelay(r, delay, interval, unit);
			}
			else
			{
				throw new InvalidOperationException(MessageFormat.Format("Scheduler shutting down, ignoring scheduling of [{}]"
					, callable));
			}
		}

		private sealed class _Runnable_98 : Runnable
		{
			public _Runnable_98(SchedulerService _enclosing, Callable<object> callable)
			{
				this._enclosing = _enclosing;
				this.callable = callable;
			}

			public void Run()
			{
				string instrName = callable.GetType().Name;
				Instrumentation instr = this._enclosing.GetServer().Get<Instrumentation>();
				if (this._enclosing.GetServer().GetStatus() == Server.Status.Halted)
				{
					Org.Apache.Hadoop.Lib.Service.Scheduler.SchedulerService.Log.Debug("Skipping [{}], server status [{}]"
						, callable, this._enclosing.GetServer().GetStatus());
					instr.Incr(Org.Apache.Hadoop.Lib.Service.Scheduler.SchedulerService.InstGroup, instrName
						 + ".skips", 1);
				}
				else
				{
					Org.Apache.Hadoop.Lib.Service.Scheduler.SchedulerService.Log.Debug("Executing [{}]"
						, callable);
					instr.Incr(Org.Apache.Hadoop.Lib.Service.Scheduler.SchedulerService.InstGroup, instrName
						 + ".execs", 1);
					Instrumentation.Cron cron = instr.CreateCron().Start();
					try
					{
						callable.Call();
					}
					catch (Exception ex)
					{
						instr.Incr(Org.Apache.Hadoop.Lib.Service.Scheduler.SchedulerService.InstGroup, instrName
							 + ".fails", 1);
						Org.Apache.Hadoop.Lib.Service.Scheduler.SchedulerService.Log.Error("Error executing [{}], {}"
							, new object[] { callable, ex.Message, ex });
					}
					finally
					{
						instr.AddCron(Org.Apache.Hadoop.Lib.Service.Scheduler.SchedulerService.InstGroup, 
							instrName, cron.Stop());
					}
				}
			}

			private readonly SchedulerService _enclosing;

			private readonly Callable<object> callable;
		}

		public virtual void Schedule(Runnable runnable, long delay, long interval, TimeUnit
			 unit)
		{
			Schedule((Callable<object>)new RunnableCallable(runnable), delay, interval, unit);
		}
	}
}
