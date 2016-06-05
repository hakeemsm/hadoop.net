using System;
using System.Collections;
using System.IO;
using System.Text;
using Javax.WS.RS.Core;
using Javax.WS.RS.Ext;
using Org.Codehaus.Jackson.Map;

using Reflect;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>
	/// Jersey provider that converts <code>Map</code>s and <code>List</code>s
	/// to their JSON representation.
	/// </summary>
	public class KMSJSONWriter : MessageBodyWriter<object>
	{
		public virtual bool IsWriteable(Type aClass, Type type, Annotation.Annotation
			[] annotations, MediaType mediaType)
		{
			return typeof(IDictionary).IsAssignableFrom(aClass) || typeof(IList).IsAssignableFrom
				(aClass);
		}

		public virtual long GetSize(object obj, Type aClass, Type type, Annotation.Annotation
			[] annotations, MediaType mediaType)
		{
			return -1;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.WS.RS.WebApplicationException"/>
		public virtual void WriteTo(object obj, Type aClass, Type type, Annotation.Annotation
			[] annotations, MediaType mediaType, MultivaluedMap<string, object> stringObjectMultivaluedMap
			, OutputStream outputStream)
		{
			TextWriter writer = new OutputStreamWriter(outputStream, Extensions.GetEncoding
				("UTF-8"));
			ObjectMapper jsonMapper = new ObjectMapper();
			jsonMapper.WriterWithDefaultPrettyPrinter().WriteValue(writer, obj);
		}
	}
}
