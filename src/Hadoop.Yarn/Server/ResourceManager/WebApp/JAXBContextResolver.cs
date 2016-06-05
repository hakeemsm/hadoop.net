using System;
using System.Collections.Generic;
using Com.Sun.Jersey.Api.Json;
using Javax.WS.RS.Ext;
using Javax.Xml.Bind;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class JAXBContextResolver : ContextResolver<JAXBContext>
	{
		private readonly IDictionary<Type, JAXBContext> typesContextMap;

		/// <exception cref="System.Exception"/>
		public JAXBContextResolver()
		{
			JAXBContext context;
			JAXBContext unWrappedRootContext;
			// you have to specify all the dao classes here
			Type[] cTypes = new Type[] { typeof(AppInfo), typeof(AppAttemptInfo), typeof(AppAttemptsInfo
				), typeof(ClusterInfo), typeof(CapacitySchedulerQueueInfo), typeof(FifoSchedulerInfo
				), typeof(SchedulerTypeInfo), typeof(NodeInfo), typeof(UserMetricsInfo), typeof(
				CapacitySchedulerInfo), typeof(ClusterMetricsInfo), typeof(SchedulerInfo), typeof(
				AppsInfo), typeof(NodesInfo), typeof(RemoteExceptionData), typeof(CapacitySchedulerQueueInfoList
				), typeof(ResourceInfo), typeof(UsersInfo), typeof(UserInfo), typeof(ApplicationStatisticsInfo
				), typeof(StatisticsItemInfo) };
			// these dao classes need root unwrapping
			Type[] rootUnwrappedTypes = new Type[] { typeof(NewApplication), typeof(ApplicationSubmissionContextInfo
				), typeof(ContainerLaunchContextInfo), typeof(LocalResourceInfo), typeof(DelegationToken
				), typeof(AppQueue) };
			this.typesContextMap = new Dictionary<Type, JAXBContext>();
			context = new JSONJAXBContext(JSONConfiguration.Natural().RootUnwrapping(false).Build
				(), cTypes);
			unWrappedRootContext = new JSONJAXBContext(JSONConfiguration.Natural().RootUnwrapping
				(true).Build(), rootUnwrappedTypes);
			foreach (Type type in cTypes)
			{
				typesContextMap[type] = context;
			}
			foreach (Type type_1 in rootUnwrappedTypes)
			{
				typesContextMap[type_1] = unWrappedRootContext;
			}
		}

		public virtual JAXBContext GetContext(Type objectType)
		{
			return typesContextMap[objectType];
		}
	}
}
