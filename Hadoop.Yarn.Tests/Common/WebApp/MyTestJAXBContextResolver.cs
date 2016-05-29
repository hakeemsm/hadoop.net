using System;
using System.Collections.Generic;
using Com.Sun.Jersey.Api.Json;
using Javax.WS.RS.Ext;
using Javax.Xml.Bind;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public class MyTestJAXBContextResolver : ContextResolver<JAXBContext>
	{
		private JAXBContext context;

		private readonly ICollection<Type> types;

		private readonly Type[] cTypes = new Type[] { typeof(MyTestWebService.MyInfo) };

		/// <exception cref="System.Exception"/>
		public MyTestJAXBContextResolver()
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
