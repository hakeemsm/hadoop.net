using System;
using System.Collections.Generic;
using Com.Sun.Jersey.Api.Json;
using Javax.WS.RS.Ext;
using Javax.Xml.Bind;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class JAXBContextResolver : ContextResolver<JAXBContext>
	{
		private JAXBContext context;

		private readonly ICollection<Type> types;

		private readonly Type[] cTypes = new Type[] { typeof(HistoryInfo), typeof(JobInfo
			), typeof(JobsInfo), typeof(TaskInfo), typeof(TasksInfo), typeof(TaskAttemptsInfo
			), typeof(ConfInfo), typeof(CounterInfo), typeof(JobTaskCounterInfo), typeof(JobTaskAttemptCounterInfo
			), typeof(TaskCounterInfo), typeof(JobCounterInfo), typeof(ReduceTaskAttemptInfo
			), typeof(TaskAttemptInfo), typeof(TaskAttemptsInfo), typeof(CounterGroupInfo), 
			typeof(TaskCounterGroupInfo), typeof(AMAttemptInfo), typeof(AMAttemptsInfo), typeof(
			RemoteExceptionData) };

		/// <exception cref="System.Exception"/>
		public JAXBContextResolver()
		{
			// you have to specify all the dao classes here
			this.types = new HashSet<Type>(Arrays.AsList(cTypes));
			this.context = new JSONJAXBContext(JSONConfiguration.Natural().RootUnwrapping(false
				).Build(), cTypes);
		}

		public virtual JAXBContext GetContext(Type objectType)
		{
			return (types.Contains(objectType)) ? context : null;
		}
	}
}
