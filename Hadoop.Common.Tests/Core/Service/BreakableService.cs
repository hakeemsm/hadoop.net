using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Service
{
	/// <summary>
	/// This is a service that can be configured to break on any of the lifecycle
	/// events, so test the failure handling of other parts of the service
	/// infrastructure.
	/// </summary>
	/// <remarks>
	/// This is a service that can be configured to break on any of the lifecycle
	/// events, so test the failure handling of other parts of the service
	/// infrastructure.
	/// It retains a counter to the number of times each entry point is called -
	/// these counters are incremented before the exceptions are raised and
	/// before the superclass state methods are invoked.
	/// </remarks>
	public class BreakableService : AbstractService
	{
		private bool failOnInit;

		private bool failOnStart;

		private bool failOnStop;

		private int[] counts = new int[4];

		public BreakableService()
			: this(false, false, false)
		{
		}

		public BreakableService(bool failOnInit, bool failOnStart, bool failOnStop)
			: base("BreakableService")
		{
			this.failOnInit = failOnInit;
			this.failOnStart = failOnStart;
			this.failOnStop = failOnStop;
			Inc(Service.STATE.Notinited);
		}

		private int Convert(Service.STATE state)
		{
			return state.GetValue();
		}

		private void Inc(Service.STATE state)
		{
			int index = Convert(state);
			counts[index]++;
		}

		public virtual int GetCount(Service.STATE state)
		{
			return counts[Convert(state)];
		}

		private void MaybeFail(bool fail, string action)
		{
			if (fail)
			{
				throw new BreakableService.BrokenLifecycleEvent(this, action);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void ServiceInit(Configuration conf)
		{
			Inc(Service.STATE.Inited);
			MaybeFail(failOnInit, "init");
			base.ServiceInit(conf);
		}

		protected internal override void ServiceStart()
		{
			Inc(Service.STATE.Started);
			MaybeFail(failOnStart, "start");
		}

		protected internal override void ServiceStop()
		{
			Inc(Service.STATE.Stopped);
			MaybeFail(failOnStop, "stop");
		}

		public virtual void SetFailOnInit(bool failOnInit)
		{
			this.failOnInit = failOnInit;
		}

		public virtual void SetFailOnStart(bool failOnStart)
		{
			this.failOnStart = failOnStart;
		}

		public virtual void SetFailOnStop(bool failOnStop)
		{
			this.failOnStop = failOnStop;
		}

		/// <summary>The exception explicitly raised on a failure</summary>
		[System.Serializable]
		public class BrokenLifecycleEvent : RuntimeException
		{
			internal readonly Service.STATE state;

			public BrokenLifecycleEvent(Org.Apache.Hadoop.Service.Service service, string action
				)
				: base("Lifecycle Failure during " + action + " state is " + service.GetServiceState
					())
			{
				state = service.GetServiceState();
			}
		}
	}
}
