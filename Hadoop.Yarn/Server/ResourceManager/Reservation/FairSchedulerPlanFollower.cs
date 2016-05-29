using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class FairSchedulerPlanFollower : AbstractSchedulerPlanFollower
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(FairSchedulerPlanFollower
			));

		private FairScheduler fs;

		public override void Init(Clock clock, ResourceScheduler sched, ICollection<Plan>
			 plans)
		{
			base.Init(clock, sched, plans);
			fs = (FairScheduler)sched;
			Log.Info("Initializing Plan Follower Policy:" + this.GetType().GetCanonicalName()
				);
		}

		protected internal override Queue GetPlanQueue(string planQueueName)
		{
			Queue planQueue = fs.GetQueueManager().GetParentQueue(planQueueName, false);
			if (planQueue == null)
			{
				Log.Error("The queue " + planQueueName + " cannot be found or is not a " + "ParentQueue"
					);
			}
			return planQueue;
		}

		protected internal override float CalculateReservationToPlanRatio(Resource clusterResources
			, Resource planResources, Resource capToAssign)
		{
			return Resources.Divide(fs.GetResourceCalculator(), clusterResources, capToAssign
				, planResources);
		}

		protected internal override bool ArePlanResourcesLessThanReservations(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource planResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 reservedResources)
		{
			return Resources.GreaterThan(fs.GetResourceCalculator(), clusterResources, reservedResources
				, planResources);
		}

		protected internal override IList<Queue> GetChildReservationQueues(Queue queue)
		{
			FSQueue planQueue = (FSQueue)queue;
			IList<FSQueue> childQueues = planQueue.GetChildQueues();
			return childQueues;
		}

		protected internal override void AddReservationQueue(string planQueueName, Queue 
			queue, string currResId)
		{
			string leafQueueName = GetReservationQueueName(planQueueName, currResId);
			fs.GetQueueManager().GetLeafQueue(leafQueueName, true);
		}

		protected internal override void CreateDefaultReservationQueue(string planQueueName
			, Queue queue, string defReservationId)
		{
			string defReservationQueueName = GetReservationQueueName(planQueueName, defReservationId
				);
			if (!fs.GetQueueManager().Exists(defReservationQueueName))
			{
				fs.GetQueueManager().GetLeafQueue(defReservationQueueName, true);
			}
		}

		protected internal override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetPlanResources
			(Plan plan, Queue queue, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResources
			)
		{
			FSParentQueue planQueue = (FSParentQueue)queue;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource planResources = planQueue.GetSteadyFairShare
				();
			return planResources;
		}

		protected internal override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetReservationQueueResourceIfExists
			(Plan plan, ReservationId reservationId)
		{
			string reservationQueueName = GetReservationQueueName(plan.GetQueueName(), reservationId
				.ToString());
			FSLeafQueue reservationQueue = fs.GetQueueManager().GetLeafQueue(reservationQueueName
				, false);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource reservationResource = null;
			if (reservationQueue != null)
			{
				reservationResource = reservationQueue.GetSteadyFairShare();
			}
			return reservationResource;
		}

		protected internal override string GetReservationQueueName(string planQueueName, 
			string reservationQueueName)
		{
			string planQueueNameFullPath = fs.GetQueueManager().GetQueue(planQueueName).GetName
				();
			if (!reservationQueueName.StartsWith(planQueueNameFullPath))
			{
				// If name is not a path we need full path for FairScheduler. See
				// YARN-2773 for the root cause
				return planQueueNameFullPath + "." + reservationQueueName;
			}
			return reservationQueueName;
		}

		protected internal override string GetReservationIdFromQueueName(string resQueueName
			)
		{
			return Sharpen.Runtime.Substring(resQueueName, resQueueName.LastIndexOf(".") + 1);
		}
	}
}
