using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Utilities shared by schedulers.</summary>
	public class SchedulerUtils
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		public const string ReleasedContainer = "Container released by application";

		public const string LostContainer = "Container released on a *lost* node";

		public const string PreemptedContainer = "Container preempted by scheduler";

		public const string CompletedApplication = "Container of a completed application";

		public const string ExpiredContainer = "Container expired since it was unused";

		public const string UnreservedContainer = "Container reservation no longer required.";

		/// <summary>
		/// Utility to create a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus"/>
		/// during exceptional
		/// circumstances.
		/// </summary>
		/// <param name="containerId">
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// of returned/released/lost container.
		/// </param>
		/// <param name="diagnostics">diagnostic message</param>
		/// <returns>
		/// <code>ContainerStatus</code> for an returned/released/lost
		/// container
		/// </returns>
		public static ContainerStatus CreateAbnormalContainerStatus(ContainerId containerId
			, string diagnostics)
		{
			return CreateAbnormalContainerStatus(containerId, ContainerExitStatus.Aborted, diagnostics
				);
		}

		/// <summary>
		/// Utility to create a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus"/>
		/// during exceptional
		/// circumstances.
		/// </summary>
		/// <param name="containerId">
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// of returned/released/lost container.
		/// </param>
		/// <param name="diagnostics">diagnostic message</param>
		/// <returns>
		/// <code>ContainerStatus</code> for an returned/released/lost
		/// container
		/// </returns>
		public static ContainerStatus CreatePreemptedContainerStatus(ContainerId containerId
			, string diagnostics)
		{
			return CreateAbnormalContainerStatus(containerId, ContainerExitStatus.Preempted, 
				diagnostics);
		}

		/// <summary>
		/// Utility to create a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus"/>
		/// during exceptional
		/// circumstances.
		/// </summary>
		/// <param name="containerId">
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// of returned/released/lost container.
		/// </param>
		/// <param name="diagnostics">diagnostic message</param>
		/// <returns>
		/// <code>ContainerStatus</code> for an returned/released/lost
		/// container
		/// </returns>
		private static ContainerStatus CreateAbnormalContainerStatus(ContainerId containerId
			, int exitStatus, string diagnostics)
		{
			ContainerStatus containerStatus = recordFactory.NewRecordInstance<ContainerStatus
				>();
			containerStatus.SetContainerId(containerId);
			containerStatus.SetDiagnostics(diagnostics);
			containerStatus.SetExitStatus(exitStatus);
			containerStatus.SetState(ContainerState.Complete);
			return containerStatus;
		}

		/// <summary>
		/// Utility method to normalize a list of resource requests, by insuring that
		/// the memory for each request is a multiple of minMemory and is not zero.
		/// </summary>
		public static void NormalizeRequests(IList<ResourceRequest> asks, ResourceCalculator
			 resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource)
		{
			foreach (ResourceRequest ask in asks)
			{
				NormalizeRequest(ask, resourceCalculator, clusterResource, minimumResource, maximumResource
					, minimumResource);
			}
		}

		/// <summary>
		/// Utility method to normalize a resource request, by insuring that the
		/// requested memory is a multiple of minMemory and is not zero.
		/// </summary>
		public static void NormalizeRequest(ResourceRequest ask, ResourceCalculator resourceCalculator
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource maximumResource)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource normalized = Resources.Normalize(resourceCalculator
				, ask.GetCapability(), minimumResource, maximumResource, minimumResource);
			ask.SetCapability(normalized);
		}

		/// <summary>
		/// Utility method to normalize a list of resource requests, by insuring that
		/// the memory for each request is a multiple of minMemory and is not zero.
		/// </summary>
		public static void NormalizeRequests(IList<ResourceRequest> asks, ResourceCalculator
			 resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource incrementResource)
		{
			foreach (ResourceRequest ask in asks)
			{
				NormalizeRequest(ask, resourceCalculator, clusterResource, minimumResource, maximumResource
					, incrementResource);
			}
		}

		/// <summary>
		/// Utility method to normalize a resource request, by insuring that the
		/// requested memory is a multiple of minMemory and is not zero.
		/// </summary>
		public static void NormalizeRequest(ResourceRequest ask, ResourceCalculator resourceCalculator
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 minimumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource maximumResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 incrementResource)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource normalized = Resources.Normalize(resourceCalculator
				, ask.GetCapability(), minimumResource, maximumResource, incrementResource);
			ask.SetCapability(normalized);
		}

		private static void NormalizeNodeLabelExpressionInRequest(ResourceRequest resReq, 
			QueueInfo queueInfo)
		{
			string labelExp = resReq.GetNodeLabelExpression();
			// if queue has default label expression, and RR doesn't have, use the
			// default label expression of queue
			if (labelExp == null && queueInfo != null && ResourceRequest.Any.Equals(resReq.GetResourceName
				()))
			{
				labelExp = queueInfo.GetDefaultNodeLabelExpression();
			}
			// If labelExp still equals to null, set it to be NO_LABEL
			if (labelExp == null)
			{
				labelExp = RMNodeLabelsManager.NoLabel;
			}
			resReq.SetNodeLabelExpression(labelExp);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceRequestException
		/// 	"/>
		public static void NormalizeAndValidateRequest(ResourceRequest resReq, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, string queueName, YarnScheduler scheduler, bool isRecovery, RMContext
			 rmContext, QueueInfo queueInfo)
		{
			if (queueInfo == null)
			{
				try
				{
					queueInfo = scheduler.GetQueueInfo(queueName, false, false);
				}
				catch (IOException)
				{
				}
			}
			// it is possible queue cannot get when queue mapping is set, just ignore
			// the queueInfo here, and move forward
			SchedulerUtils.NormalizeNodeLabelExpressionInRequest(resReq, queueInfo);
			if (!isRecovery)
			{
				ValidateResourceRequest(resReq, maximumResource, queueInfo, rmContext);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceRequestException
		/// 	"/>
		public static void NormalizeAndValidateRequest(ResourceRequest resReq, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, string queueName, YarnScheduler scheduler, bool isRecovery, RMContext
			 rmContext)
		{
			NormalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler, isRecovery
				, rmContext, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceRequestException
		/// 	"/>
		public static void NormalizeAndvalidateRequest(ResourceRequest resReq, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, string queueName, YarnScheduler scheduler, RMContext rmContext
			, QueueInfo queueInfo)
		{
			NormalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler, false, 
				rmContext, queueInfo);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceRequestException
		/// 	"/>
		public static void NormalizeAndvalidateRequest(ResourceRequest resReq, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, string queueName, YarnScheduler scheduler, RMContext rmContext
			)
		{
			NormalizeAndValidateRequest(resReq, maximumResource, queueName, scheduler, false, 
				rmContext, null);
		}

		/// <summary>
		/// Utility method to validate a resource request, by insuring that the
		/// requested memory/vcore is non-negative and not greater than max
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceRequestException
		/// 	">when there is invalid request</exception>
		private static void ValidateResourceRequest(ResourceRequest resReq, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maximumResource, QueueInfo queueInfo, RMContext rmContext)
		{
			if (resReq.GetCapability().GetMemory() < 0 || resReq.GetCapability().GetMemory() 
				> maximumResource.GetMemory())
			{
				throw new InvalidResourceRequestException("Invalid resource request" + ", requested memory < 0"
					 + ", or requested memory > max configured" + ", requestedMemory=" + resReq.GetCapability
					().GetMemory() + ", maxMemory=" + maximumResource.GetMemory());
			}
			if (resReq.GetCapability().GetVirtualCores() < 0 || resReq.GetCapability().GetVirtualCores
				() > maximumResource.GetVirtualCores())
			{
				throw new InvalidResourceRequestException("Invalid resource request" + ", requested virtual cores < 0"
					 + ", or requested virtual cores > max configured" + ", requestedVirtualCores=" 
					+ resReq.GetCapability().GetVirtualCores() + ", maxVirtualCores=" + maximumResource
					.GetVirtualCores());
			}
			string labelExp = resReq.GetNodeLabelExpression();
			// we don't allow specify label expression other than resourceName=ANY now
			if (!ResourceRequest.Any.Equals(resReq.GetResourceName()) && labelExp != null && 
				!labelExp.Trim().IsEmpty())
			{
				throw new InvalidResourceRequestException("Invailid resource request, queue=" + queueInfo
					.GetQueueName() + " specified node label expression in a " + "resource request has resource name = "
					 + resReq.GetResourceName());
			}
			// we don't allow specify label expression with more than one node labels now
			if (labelExp != null && labelExp.Contains("&&"))
			{
				throw new InvalidResourceRequestException("Invailid resource request, queue=" + queueInfo
					.GetQueueName() + " specified more than one node label " + "in a node label expression, node label expression = "
					 + labelExp);
			}
			if (labelExp != null && !labelExp.Trim().IsEmpty() && queueInfo != null)
			{
				if (!CheckQueueLabelExpression(queueInfo.GetAccessibleNodeLabels(), labelExp, rmContext
					))
				{
					throw new InvalidResourceRequestException("Invalid resource request" + ", queue="
						 + queueInfo.GetQueueName() + " doesn't have permission to access all labels " +
						 "in resource request. labelExpression of resource request=" + labelExp + ". Queue labels="
						 + (queueInfo.GetAccessibleNodeLabels() == null ? string.Empty : StringUtils.Join
						(queueInfo.GetAccessibleNodeLabels().GetEnumerator(), ',')));
				}
			}
		}

		public static bool CheckQueueAccessToNode(ICollection<string> queueLabels, ICollection
			<string> nodeLabels)
		{
			// if queue's label is *, it can access any node
			if (queueLabels != null && queueLabels.Contains(RMNodeLabelsManager.Any))
			{
				return true;
			}
			// any queue can access to a node without label
			if (nodeLabels == null || nodeLabels.IsEmpty())
			{
				return true;
			}
			// a queue can access to a node only if it contains any label of the node
			if (queueLabels != null && Sets.Intersection(queueLabels, nodeLabels).Count > 0)
			{
				return true;
			}
			// sorry, you cannot access
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckIfLabelInClusterNodeLabels(RMNodeLabelsManager mgr, ICollection
			<string> labels)
		{
			if (mgr == null)
			{
				if (labels != null && !labels.IsEmpty())
				{
					throw new IOException("NodeLabelManager is null, please check");
				}
				return;
			}
			if (labels != null)
			{
				foreach (string label in labels)
				{
					if (!label.Equals(RMNodeLabelsManager.Any) && !mgr.ContainsNodeLabel(label))
					{
						throw new IOException("NodeLabelManager doesn't include label = " + label + ", please check."
							);
					}
				}
			}
		}

		public static bool CheckNodeLabelExpression(ICollection<string> nodeLabels, string
			 labelExpression)
		{
			// empty label expression can only allocate on node with empty labels
			if (labelExpression == null || labelExpression.Trim().IsEmpty())
			{
				if (!nodeLabels.IsEmpty())
				{
					return false;
				}
			}
			if (labelExpression != null)
			{
				foreach (string str in labelExpression.Split("&&"))
				{
					if (!str.Trim().IsEmpty() && (nodeLabels == null || !nodeLabels.Contains(str.Trim
						())))
					{
						return false;
					}
				}
			}
			return true;
		}

		/// <summary>
		/// Check queue label expression, check if node label in queue's
		/// node-label-expression existed in clusterNodeLabels if rmContext != null
		/// </summary>
		public static bool CheckQueueLabelExpression(ICollection<string> queueLabels, string
			 labelExpression, RMContext rmContext)
		{
			// if label expression is empty, we can allocate container on any node
			if (labelExpression == null)
			{
				return true;
			}
			foreach (string str in labelExpression.Split("&&"))
			{
				str = str.Trim();
				if (!str.Trim().IsEmpty())
				{
					// check queue label
					if (queueLabels == null)
					{
						return false;
					}
					else
					{
						if (!queueLabels.Contains(str) && !queueLabels.Contains(RMNodeLabelsManager.Any))
						{
							return false;
						}
					}
					// check node label manager contains this label
					if (null != rmContext)
					{
						RMNodeLabelsManager nlm = rmContext.GetNodeLabelManager();
						if (nlm != null && !nlm.ContainsNodeLabel(str))
						{
							return false;
						}
					}
				}
			}
			return true;
		}

		public static AccessType ToAccessType(QueueACL acl)
		{
			switch (acl)
			{
				case QueueACL.AdministerQueue:
				{
					return AccessType.AdministerQueue;
				}

				case QueueACL.SubmitApplications:
				{
					return AccessType.SubmitApp;
				}
			}
			return null;
		}
	}
}
