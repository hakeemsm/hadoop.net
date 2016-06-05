using System;
using System.Collections.Generic;
using Com.Sun.Jersey.Api.Json;
using Javax.WS.RS.Ext;
using Javax.Xml.Bind;
using Org.Apache.Hadoop.Yarn.Server.Webapp.Dao;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class JAXBContextResolver : ContextResolver<JAXBContext>
	{
		private JAXBContext context;

		private readonly ICollection<Type> types;

		private readonly Type[] cTypes = new Type[] { typeof(AppInfo), typeof(AppsInfo), 
			typeof(AppAttemptInfo), typeof(AppAttemptsInfo), typeof(ContainerInfo), typeof(ContainersInfo
			) };

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
