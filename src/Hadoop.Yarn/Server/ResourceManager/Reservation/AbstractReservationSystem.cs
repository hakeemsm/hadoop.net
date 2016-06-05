using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This is the implementation of
	/// <see cref="ReservationSystem"/>
	/// based on the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
	/// 	"/>
	/// </summary>
	public abstract class AbstractReservationSystem : AbstractService, ReservationSystem
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.AbstractReservationSystem
			));

		private readonly ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock
			(true);

		private readonly Lock readLock = readWriteLock.ReadLock();

		private readonly Lock writeLock = readWriteLock.WriteLock();

		private bool initialized = false;

		private readonly Clock clock = new UTCClock();

		private AtomicLong resCounter = new AtomicLong();

		private IDictionary<string, Plan> plans = new Dictionary<string, Plan>();

		private IDictionary<ReservationId, string> resQMap = new Dictionary<ReservationId
			, string>();

		private RMContext rmContext;

		private ResourceScheduler scheduler;

		private ScheduledExecutorService scheduledExecutorService;

		protected internal Configuration conf;

		protected internal long planStepSize;

		private PlanFollower planFollower;

		/// <summary>Construct the service.</summary>
		/// <param name="name">service name</param>
		public AbstractReservationSystem(string name)
			: base(name)
		{
		}

		// private static final String DEFAULT_CAPACITY_SCHEDULER_PLAN
		public virtual void SetRMContext(RMContext rmContext)
		{
			writeLock.Lock();
			try
			{
				this.rmContext = rmContext;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void Reinitialize(Configuration conf, RMContext rmContext)
		{
			writeLock.Lock();
			try
			{
				if (!initialized)
				{
					Initialize(conf);
					initialized = true;
				}
				else
				{
					InitializeNewPlans(conf);
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void Initialize(Configuration conf)
		{
			Log.Info("Initializing Reservation system");
			this.conf = conf;
			scheduler = rmContext.GetScheduler();
			// Get the plan step size
			planStepSize = conf.GetTimeDuration(YarnConfiguration.RmReservationSystemPlanFollowerTimeStep
				, YarnConfiguration.DefaultRmReservationSystemPlanFollowerTimeStep, TimeUnit.Milliseconds
				);
			if (planStepSize < 0)
			{
				planStepSize = YarnConfiguration.DefaultRmReservationSystemPlanFollowerTimeStep;
			}
			// Create a plan corresponding to every reservable queue
			ICollection<string> planQueueNames = scheduler.GetPlanQueues();
			foreach (string planQueueName in planQueueNames)
			{
				Plan plan = InitializePlan(planQueueName);
				plans[planQueueName] = plan;
			}
		}

		private void InitializeNewPlans(Configuration conf)
		{
			Log.Info("Refreshing Reservation system");
			writeLock.Lock();
			try
			{
				// Create a plan corresponding to every new reservable queue
				ICollection<string> planQueueNames = scheduler.GetPlanQueues();
				foreach (string planQueueName in planQueueNames)
				{
					if (!plans.Contains(planQueueName))
					{
						Plan plan = InitializePlan(planQueueName);
						plans[planQueueName] = plan;
					}
					else
					{
						Log.Warn("Plan based on reservation queue {0} already exists.", planQueueName);
					}
				}
				// Update the plan follower with the active plans
				if (planFollower != null)
				{
					planFollower.SetPlans(plans.Values);
				}
			}
			catch (YarnException e)
			{
				Log.Warn("Exception while trying to refresh reservable queues", e);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		private PlanFollower CreatePlanFollower()
		{
			string planFollowerPolicyClassName = conf.Get(YarnConfiguration.RmReservationSystemPlanFollower
				, GetDefaultPlanFollower());
			if (planFollowerPolicyClassName == null)
			{
				return null;
			}
			Log.Info("Using PlanFollowerPolicy: " + planFollowerPolicyClassName);
			try
			{
				Type planFollowerPolicyClazz = conf.GetClassByName(planFollowerPolicyClassName);
				if (typeof(PlanFollower).IsAssignableFrom(planFollowerPolicyClazz))
				{
					return (PlanFollower)ReflectionUtils.NewInstance(planFollowerPolicyClazz, conf);
				}
				else
				{
					throw new YarnRuntimeException("Class: " + planFollowerPolicyClassName + " not instance of "
						 + typeof(PlanFollower).GetCanonicalName());
				}
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException("Could not instantiate PlanFollowerPolicy: " + planFollowerPolicyClassName
					, e);
			}
		}

		private string GetDefaultPlanFollower()
		{
			// currently only capacity scheduler is supported
			if (scheduler is CapacityScheduler)
			{
				return typeof(CapacitySchedulerPlanFollower).FullName;
			}
			else
			{
				if (scheduler is FairScheduler)
				{
					return typeof(FairSchedulerPlanFollower).FullName;
				}
			}
			return null;
		}

		public virtual Plan GetPlan(string planName)
		{
			readLock.Lock();
			try
			{
				return plans[planName];
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <returns>the planStepSize</returns>
		public virtual long GetPlanFollowerTimeStep()
		{
			readLock.Lock();
			try
			{
				return planStepSize;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual void SynchronizePlan(string planName)
		{
			writeLock.Lock();
			try
			{
				Plan plan = plans[planName];
				if (plan != null)
				{
					planFollower.SynchronizePlan(plan);
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			Configuration configuration = new Configuration(conf);
			Reinitialize(configuration, rmContext);
			// Create the plan follower with the active plans
			planFollower = CreatePlanFollower();
			if (planFollower != null)
			{
				planFollower.Init(clock, scheduler, plans.Values);
			}
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (planFollower != null)
			{
				scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
				scheduledExecutorService.ScheduleWithFixedDelay(planFollower, 0L, planStepSize, TimeUnit
					.Milliseconds);
			}
			base.ServiceStart();
		}

		protected override void ServiceStop()
		{
			// Stop the plan follower
			if (scheduledExecutorService != null && !scheduledExecutorService.IsShutdown())
			{
				scheduledExecutorService.Shutdown();
			}
			// Clear the plans
			plans.Clear();
		}

		public virtual string GetQueueForReservation(ReservationId reservationId)
		{
			readLock.Lock();
			try
			{
				return resQMap[reservationId];
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual void SetQueueForReservation(ReservationId reservationId, string queueName
			)
		{
			writeLock.Lock();
			try
			{
				resQMap[reservationId] = queueName;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual ReservationId GetNewReservationId()
		{
			writeLock.Lock();
			try
			{
				ReservationId resId = ReservationId.NewInstance(ResourceManager.GetClusterTimeStamp
					(), resCounter.IncrementAndGet());
				Log.Info("Allocated new reservationId: " + resId);
				return resId;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual IDictionary<string, Plan> GetAllPlans()
		{
			return plans;
		}

		/// <summary>Get the default reservation system corresponding to the scheduler</summary>
		/// <param name="scheduler">the scheduler for which the reservation system is required
		/// 	</param>
		public static string GetDefaultReservationSystem(ResourceScheduler scheduler)
		{
			if (scheduler is CapacityScheduler)
			{
				return typeof(CapacityReservationSystem).FullName;
			}
			else
			{
				if (scheduler is FairScheduler)
				{
					return typeof(FairReservationSystem).FullName;
				}
			}
			return null;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal virtual Plan InitializePlan(string planQueueName)
		{
			string planQueuePath = GetPlanQueuePath(planQueueName);
			SharingPolicy adPolicy = GetAdmissionPolicy(planQueuePath);
			adPolicy.Init(planQueuePath, GetReservationSchedulerConfiguration());
			// Calculate the max plan capacity
			Resource minAllocation = GetMinAllocation();
			Resource maxAllocation = GetMaxAllocation();
			ResourceCalculator rescCalc = GetResourceCalculator();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totCap = GetPlanQueueCapacity(planQueueName
				);
			Plan plan = new InMemoryPlan(GetRootQueueMetrics(), adPolicy, GetAgent(planQueuePath
				), totCap, planStepSize, rescCalc, minAllocation, maxAllocation, planQueueName, 
				GetReplanner(planQueuePath), GetReservationSchedulerConfiguration().GetMoveOnExpiry
				(planQueuePath));
			Log.Info("Intialized plan {0} based on reservable queue {1}", plan.ToString(), planQueueName
				);
			return plan;
		}

		protected internal virtual Planner GetReplanner(string planQueueName)
		{
			ReservationSchedulerConfiguration reservationConfig = GetReservationSchedulerConfiguration
				();
			string plannerClassName = reservationConfig.GetReplanner(planQueueName);
			Log.Info("Using Replanner: " + plannerClassName + " for queue: " + planQueueName);
			try
			{
				Type plannerClazz = conf.GetClassByName(plannerClassName);
				if (typeof(Planner).IsAssignableFrom(plannerClazz))
				{
					Planner planner = (Planner)ReflectionUtils.NewInstance(plannerClazz, conf);
					planner.Init(planQueueName, reservationConfig);
					return planner;
				}
				else
				{
					throw new YarnRuntimeException("Class: " + plannerClazz + " not instance of " + typeof(
						Planner).GetCanonicalName());
				}
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException("Could not instantiate Planner: " + plannerClassName
					 + " for queue: " + planQueueName, e);
			}
		}

		protected internal virtual ReservationAgent GetAgent(string queueName)
		{
			ReservationSchedulerConfiguration reservationConfig = GetReservationSchedulerConfiguration
				();
			string agentClassName = reservationConfig.GetReservationAgent(queueName);
			Log.Info("Using Agent: " + agentClassName + " for queue: " + queueName);
			try
			{
				Type agentClazz = conf.GetClassByName(agentClassName);
				if (typeof(ReservationAgent).IsAssignableFrom(agentClazz))
				{
					return (ReservationAgent)ReflectionUtils.NewInstance(agentClazz, conf);
				}
				else
				{
					throw new YarnRuntimeException("Class: " + agentClassName + " not instance of " +
						 typeof(ReservationAgent).GetCanonicalName());
				}
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException("Could not instantiate Agent: " + agentClassName +
					 " for queue: " + queueName, e);
			}
		}

		protected internal virtual SharingPolicy GetAdmissionPolicy(string queueName)
		{
			ReservationSchedulerConfiguration reservationConfig = GetReservationSchedulerConfiguration
				();
			string admissionPolicyClassName = reservationConfig.GetReservationAdmissionPolicy
				(queueName);
			Log.Info("Using AdmissionPolicy: " + admissionPolicyClassName + " for queue: " + 
				queueName);
			try
			{
				Type admissionPolicyClazz = conf.GetClassByName(admissionPolicyClassName);
				if (typeof(SharingPolicy).IsAssignableFrom(admissionPolicyClazz))
				{
					return (SharingPolicy)ReflectionUtils.NewInstance(admissionPolicyClazz, conf);
				}
				else
				{
					throw new YarnRuntimeException("Class: " + admissionPolicyClassName + " not instance of "
						 + typeof(SharingPolicy).GetCanonicalName());
				}
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException("Could not instantiate AdmissionPolicy: " + admissionPolicyClassName
					 + " for queue: " + queueName, e);
			}
		}

		protected internal abstract ReservationSchedulerConfiguration GetReservationSchedulerConfiguration
			();

		protected internal abstract string GetPlanQueuePath(string planQueueName);

		protected internal abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource GetPlanQueueCapacity
			(string planQueueName);

		protected internal abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinAllocation
			();

		protected internal abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaxAllocation
			();

		protected internal abstract ResourceCalculator GetResourceCalculator();

		protected internal abstract QueueMetrics GetRootQueueMetrics();
	}
}
