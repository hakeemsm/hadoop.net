using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class ReservationInputValidator
	{
		private readonly Clock clock;

		/// <summary>Utility class to validate reservation requests.</summary>
		public ReservationInputValidator(Clock clock)
		{
			this.clock = clock;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private Plan ValidateReservation(ReservationSystem reservationSystem, ReservationId
			 reservationId, string auditConstant)
		{
			string message = string.Empty;
			// check if the reservation id is valid
			if (reservationId == null)
			{
				message = "Missing reservation id." + " Please try again by specifying a reservation id.";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input", 
					"ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
			string queueName = reservationSystem.GetQueueForReservation(reservationId);
			if (queueName == null)
			{
				message = "The specified reservation with ID: " + reservationId + " is unknown. Please try again with a valid reservation.";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input", 
					"ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
			// check if the associated plan is valid
			Plan plan = reservationSystem.GetPlan(queueName);
			if (plan == null)
			{
				message = "The specified reservation: " + reservationId + " is not associated with any valid plan."
					 + " Please try again with a valid reservation.";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input", 
					"ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
			return plan;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void ValidateReservationDefinition(ReservationId reservationId, ReservationDefinition
			 contract, Plan plan, string auditConstant)
		{
			string message = string.Empty;
			// check if deadline is in the past
			if (contract == null)
			{
				message = "Missing reservation definition." + " Please try again by specifying a reservation definition.";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input definition"
					, "ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
			if (contract.GetDeadline() <= clock.GetTime())
			{
				message = "The specified deadline: " + contract.GetDeadline() + " is the past. Please try again with deadline in the future.";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input definition"
					, "ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
			// Check if at least one RR has been specified
			ReservationRequests resReqs = contract.GetReservationRequests();
			if (resReqs == null)
			{
				message = "No resources have been specified to reserve." + "Please try again by specifying the resources to reserve.";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input definition"
					, "ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
			IList<ReservationRequest> resReq = resReqs.GetReservationResources();
			if (resReq == null || resReq.IsEmpty())
			{
				message = "No resources have been specified to reserve." + " Please try again by specifying the resources to reserve.";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input definition"
					, "ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
			// compute minimum duration and max gang size
			long minDuration = 0;
			Resource maxGangSize = Resource.NewInstance(0, 0);
			ReservationRequestInterpreter type = contract.GetReservationRequests().GetInterpreter
				();
			foreach (ReservationRequest rr in resReq)
			{
				if (type == ReservationRequestInterpreter.RAll || type == ReservationRequestInterpreter
					.RAny)
				{
					minDuration = Math.Max(minDuration, rr.GetDuration());
				}
				else
				{
					minDuration += rr.GetDuration();
				}
				maxGangSize = Resources.Max(plan.GetResourceCalculator(), plan.GetTotalCapacity()
					, maxGangSize, Resources.Multiply(rr.GetCapability(), rr.GetConcurrency()));
			}
			// verify the allocation is possible (skip for ANY)
			if (contract.GetDeadline() - contract.GetArrival() < minDuration && type != ReservationRequestInterpreter
				.RAny)
			{
				message = "The time difference (" + (contract.GetDeadline() - contract.GetArrival
					()) + ") between arrival (" + contract.GetArrival() + ") " + "and deadline (" + 
					contract.GetDeadline() + ") must " + " be greater or equal to the minimum resource duration ("
					 + minDuration + ")";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input definition"
					, "ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
			// check that the largest gang does not exceed the inventory available
			// capacity (skip for ANY)
			if (Resources.GreaterThan(plan.GetResourceCalculator(), plan.GetTotalCapacity(), 
				maxGangSize, plan.GetTotalCapacity()) && type != ReservationRequestInterpreter.RAny)
			{
				message = "The size of the largest gang in the reservation refinition (" + maxGangSize
					 + ") exceed the capacity available (" + plan.GetTotalCapacity() + " )";
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, "validate reservation input definition"
					, "ClientRMService", message);
				throw RPCUtil.GetRemoteException(message);
			}
		}

		/// <summary>
		/// Quick validation on the input to check some obvious fail conditions (fail
		/// fast) the input and returns the appropriate
		/// <see cref="Plan"/>
		/// associated with
		/// the specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Queue"/>
		/// or throws an exception message illustrating the
		/// details of any validation check failures
		/// </summary>
		/// <param name="reservationSystem">
		/// the
		/// <see cref="ReservationSystem"/>
		/// to validate against
		/// </param>
		/// <param name="request">
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationSubmissionRequest
		/// 	"/>
		/// defining the
		/// resources required over time for the request
		/// </param>
		/// <param name="reservationId">
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// associated with the current
		/// request
		/// </param>
		/// <returns>
		/// the
		/// <see cref="Plan"/>
		/// to submit the request to
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual Plan ValidateReservationSubmissionRequest(ReservationSystem reservationSystem
			, ReservationSubmissionRequest request, ReservationId reservationId)
		{
			// Check if it is a managed queue
			string queueName = request.GetQueue();
			if (queueName == null || queueName.IsEmpty())
			{
				string errMsg = "The queue to submit is not specified." + " Please try again with a valid reservable queue.";
				RMAuditLogger.LogFailure("UNKNOWN", RMAuditLogger.AuditConstants.SubmitReservationRequest
					, "validate reservation input", "ClientRMService", errMsg);
				throw RPCUtil.GetRemoteException(errMsg);
			}
			Plan plan = reservationSystem.GetPlan(queueName);
			if (plan == null)
			{
				string errMsg = "The specified queue: " + queueName + " is not managed by reservation system."
					 + " Please try again with a valid reservable queue.";
				RMAuditLogger.LogFailure("UNKNOWN", RMAuditLogger.AuditConstants.SubmitReservationRequest
					, "validate reservation input", "ClientRMService", errMsg);
				throw RPCUtil.GetRemoteException(errMsg);
			}
			ValidateReservationDefinition(reservationId, request.GetReservationDefinition(), 
				plan, RMAuditLogger.AuditConstants.SubmitReservationRequest);
			return plan;
		}

		/// <summary>
		/// Quick validation on the input to check some obvious fail conditions (fail
		/// fast) the input and returns the appropriate
		/// <see cref="Plan"/>
		/// associated with
		/// the specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Queue"/>
		/// or throws an exception message illustrating the
		/// details of any validation check failures
		/// </summary>
		/// <param name="reservationSystem">
		/// the
		/// <see cref="ReservationSystem"/>
		/// to validate against
		/// </param>
		/// <param name="request">
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationUpdateRequest"/>
		/// defining the resources
		/// required over time for the request
		/// </param>
		/// <returns>
		/// the
		/// <see cref="Plan"/>
		/// to submit the request to
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual Plan ValidateReservationUpdateRequest(ReservationSystem reservationSystem
			, ReservationUpdateRequest request)
		{
			ReservationId reservationId = request.GetReservationId();
			Plan plan = ValidateReservation(reservationSystem, reservationId, RMAuditLogger.AuditConstants
				.UpdateReservationRequest);
			ValidateReservationDefinition(reservationId, request.GetReservationDefinition(), 
				plan, RMAuditLogger.AuditConstants.UpdateReservationRequest);
			return plan;
		}

		/// <summary>
		/// Quick validation on the input to check some obvious fail conditions (fail
		/// fast) the input and returns the appropriate
		/// <see cref="Plan"/>
		/// associated with
		/// the specified
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Queue"/>
		/// or throws an exception message illustrating the
		/// details of any validation check failures
		/// </summary>
		/// <param name="reservationSystem">
		/// the
		/// <see cref="ReservationSystem"/>
		/// to validate against
		/// </param>
		/// <param name="request">
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationDeleteRequest"/>
		/// defining the resources
		/// required over time for the request
		/// </param>
		/// <returns>
		/// the
		/// <see cref="Plan"/>
		/// to submit the request to
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual Plan ValidateReservationDeleteRequest(ReservationSystem reservationSystem
			, ReservationDeleteRequest request)
		{
			return ValidateReservation(reservationSystem, request.GetReservationId(), RMAuditLogger.AuditConstants
				.DeleteReservationRequest);
		}
	}
}
