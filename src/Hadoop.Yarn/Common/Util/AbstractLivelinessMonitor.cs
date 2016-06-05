using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Service;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// A simple liveliness monitor with which clients can register, trust the
	/// component to monitor liveliness, get a call-back on expiry and then finally
	/// unregister.
	/// </summary>
	public abstract class AbstractLivelinessMonitor<O> : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Util.AbstractLivelinessMonitor
			));

		private Sharpen.Thread checkerThread;

		private volatile bool stopped;

		public const int DefaultExpire = 5 * 60 * 1000;

		private int expireInterval = DefaultExpire;

		private int monitorInterval = expireInterval / 3;

		private readonly Clock clock;

		private IDictionary<O, long> running = new Dictionary<O, long>();

		public AbstractLivelinessMonitor(string name, Clock clock)
			: base(name)
		{
			//thread which runs periodically to see the last time since a heartbeat is
			//received.
			//5 mins
			this.clock = clock;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			System.Diagnostics.Debug.Assert(!stopped, "starting when already stopped");
			ResetTimer();
			checkerThread = new Sharpen.Thread(new AbstractLivelinessMonitor.PingChecker(this
				));
			checkerThread.SetName("Ping Checker");
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

		protected internal abstract void Expire(O ob);

		protected internal virtual void SetExpireInterval(int expireInterval)
		{
			this.expireInterval = expireInterval;
		}

		protected internal virtual void SetMonitorInterval(int monitorInterval)
		{
			this.monitorInterval = monitorInterval;
		}

		public virtual void ReceivedPing(O ob)
		{
			lock (this)
			{
				//only put for the registered objects
				if (running.Contains(ob))
				{
					running[ob] = clock.GetTime();
				}
			}
		}

		public virtual void Register(O ob)
		{
			lock (this)
			{
				running[ob] = clock.GetTime();
			}
		}

		public virtual void Unregister(O ob)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(running, ob);
			}
		}

		public virtual void ResetTimer()
		{
			lock (this)
			{
				long time = clock.GetTime();
				foreach (O ob in running.Keys)
				{
					running[ob] = time;
				}
			}
		}

		private class PingChecker : Runnable
		{
			public virtual void Run()
			{
				while (!this._enclosing.stopped && !Sharpen.Thread.CurrentThread().IsInterrupted(
					))
				{
					lock (this._enclosing._enclosing)
					{
						IEnumerator<KeyValuePair<O, long>> iterator = this._enclosing.running.GetEnumerator
							();
						//avoid calculating current time everytime in loop
						long currentTime = this._enclosing.clock.GetTime();
						while (iterator.HasNext())
						{
							KeyValuePair<O, long> entry = iterator.Next();
							if (currentTime > entry.Value + this._enclosing.expireInterval)
							{
								iterator.Remove();
								this._enclosing.Expire(entry.Key);
								AbstractLivelinessMonitor.Log.Info("Expired:" + entry.Key.ToString() + " Timed out after "
									 + this._enclosing.expireInterval / 1000 + " secs");
							}
						}
					}
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.monitorInterval);
					}
					catch (Exception)
					{
						AbstractLivelinessMonitor.Log.Info(this._enclosing.GetName() + " thread interrupted"
							);
						break;
					}
				}
			}

			internal PingChecker(AbstractLivelinessMonitor<O> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly AbstractLivelinessMonitor<O> _enclosing;
		}
	}
}
