using System;
using System.Collections.Generic;
using Com.Sun.Jersey.Api.Json;
using Javax.WS.RS.Ext;
using Javax.Xml.Bind;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class JAXBContextResolver : ContextResolver<JAXBContext>
	{
		private JAXBContext context;

		private readonly ICollection<Type> types;

		private readonly Type[] cTypes = new Type[] { typeof(AppInfo), typeof(AppsInfo), 
			typeof(ContainerInfo), typeof(ContainersInfo), typeof(NodeInfo), typeof(RemoteExceptionData
			) };

		/// <exception cref="System.Exception"/>
		public JAXBContextResolver()
		{
			// you have to specify all the dao classes here
			this.types = new HashSet<Type>(Arrays.AsList(cTypes));
			// sets the json configuration so that the json output looks like
			// the xml output
			this.context = new JSONJAXBContext(JSONConfiguration.Natural().RootUnwrapping(false
				).Build(), cTypes);
		}

		public virtual JAXBContext GetContext(Type objectType)
		{
			return (types.Contains(objectType)) ? context : null;
		}
	}
}
