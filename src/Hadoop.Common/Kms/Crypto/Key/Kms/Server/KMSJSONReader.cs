using System;
using System.Collections;
using System.IO;
using Javax.WS.RS.Core;
using Javax.WS.RS.Ext;
using Org.Codehaus.Jackson.Map;

using Reflect;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	public class KMSJSONReader : MessageBodyReader<IDictionary>
	{
		public virtual bool IsReadable(Type type, Type genericType, Annotation.Annotation
			[] annotations, MediaType mediaType)
		{
			return type.IsAssignableFrom(typeof(IDictionary));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.WS.RS.WebApplicationException"/>
		public virtual IDictionary ReadFrom(Type type, Type genericType, Annotation.Annotation
			[] annotations, MediaType mediaType, MultivaluedMap<string, string> httpHeaders, 
			InputStream entityStream)
		{
			ObjectMapper mapper = new ObjectMapper();
			return mapper.ReadValue(entityStream, type);
		}
	}
}
