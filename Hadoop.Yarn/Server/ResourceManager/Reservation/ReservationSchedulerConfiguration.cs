using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public abstract class ReservationSchedulerConfiguration : Configuration
	{
		[InterfaceAudience.Private]
		public const long DefaultReservationWindow = 24 * 60 * 60 * 1000;

		[InterfaceAudience.Private]
		public const string DefaultReservationAdmissionPolicy = "org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy";

		[InterfaceAudience.Private]
		public const string DefaultReservationAgentName = "org.apache.hadoop.yarn.server.resourcemanager.reservation.GreedyReservationAgent";

		[InterfaceAudience.Private]
		public const string DefaultReservationPlannerName = "org.apache.hadoop.yarn.server.resourcemanager.reservation.SimpleCapacityReplanner";

		[InterfaceAudience.Private]
		public const bool DefaultReservationMoveOnExpiry = true;

		[InterfaceAudience.Private]
		public const long DefaultReservationEnforcementWindow = 60 * 60 * 1000;

		[InterfaceAudience.Private]
		public const bool DefaultShowReservationsAsQueues = false;

		[InterfaceAudience.Private]
		public const float DefaultCapacityOverTimeMultiplier = 1;

		public ReservationSchedulerConfiguration()
			: base()
		{
		}

		public ReservationSchedulerConfiguration(Configuration configuration)
			: base(configuration)
		{
		}

		// 1 day in msec
		// default to 1h lookahead enforcement
		// 1 hour
		/// <summary>Checks if the queue participates in reservation based scheduling</summary>
		/// <param name="queue"/>
		/// <returns>true if the queue participates in reservation based scheduling</returns>
		public abstract bool IsReservable(string queue);

		/// <summary>
		/// Gets the length of time in milliseconds for which the
		/// <see cref="SharingPolicy"/>
		/// checks for validity
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>
		/// length in time in milliseconds for which to check the
		/// <see cref="SharingPolicy"/>
		/// </returns>
		public virtual long GetReservationWindow(string queue)
		{
			return DefaultReservationWindow;
		}

		/// <summary>
		/// Gets the average allowed capacity which will aggregated over the
		/// <see cref="GetReservationWindow(string)"/>
		/// by the
		/// the
		/// <see cref="SharingPolicy"/>
		/// to check aggregate used capacity
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>
		/// average capacity allowed by the
		/// <see cref="SharingPolicy"/>
		/// </returns>
		public virtual float GetAverageCapacity(string queue)
		{
			return DefaultCapacityOverTimeMultiplier;
		}

		/// <summary>
		/// Gets the maximum capacity at any time that the
		/// <see cref="SharingPolicy"/>
		/// allows
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>maximum allowed capacity at any time</returns>
		public virtual float GetInstantaneousMaxCapacity(string queue)
		{
			return DefaultCapacityOverTimeMultiplier;
		}

		/// <summary>
		/// Gets the name of the
		/// <see cref="SharingPolicy"/>
		/// class associated with the queue
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>
		/// the class name of the
		/// <see cref="SharingPolicy"/>
		/// </returns>
		public virtual string GetReservationAdmissionPolicy(string queue)
		{
			return DefaultReservationAdmissionPolicy;
		}

		/// <summary>
		/// Gets the name of the
		/// <see cref="ReservationAgent"/>
		/// class associated with the
		/// queue
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>
		/// the class name of the
		/// <see cref="ReservationAgent"/>
		/// </returns>
		public virtual string GetReservationAgent(string queue)
		{
			return DefaultReservationAgentName;
		}

		/// <summary>Checks whether the reservation queues be hidden or visible</summary>
		/// <param name="queuePath">name of the queue</param>
		/// <returns>true if reservation queues should be visible</returns>
		public virtual bool GetShowReservationAsQueues(string queuePath)
		{
			return DefaultShowReservationsAsQueues;
		}

		/// <summary>
		/// Gets the name of the
		/// <see cref="Planner"/>
		/// class associated with the
		/// queue
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>
		/// the class name of the
		/// <see cref="Planner"/>
		/// </returns>
		public virtual string GetReplanner(string queue)
		{
			return DefaultReservationPlannerName;
		}

		/// <summary>
		/// Gets whether the applications should be killed or moved to the parent queue
		/// when the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// expires
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>
		/// true if application should be moved, false if they need to be
		/// killed
		/// </returns>
		public virtual bool GetMoveOnExpiry(string queue)
		{
			return DefaultReservationMoveOnExpiry;
		}

		/// <summary>
		/// Gets the time in milliseconds for which the
		/// <see cref="Planner"/>
		/// will verify
		/// the
		/// <see cref="Plan"/>
		/// s satisfy the constraints
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>the time in milliseconds for which to check constraints</returns>
		public virtual long GetEnforcementWindow(string queue)
		{
			return DefaultReservationEnforcementWindow;
		}
	}
}
