using System;
using Javax.WS.RS.Core;
using Org.Codehaus.Jackson.Jaxrs;
using Org.Codehaus.Jackson.Map;
using Org.Codehaus.Jackson.Map.Annotate;
using Org.Codehaus.Jackson.XC;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>
	/// YARN's implementation of JAX-RS abstractions based on
	/// <see cref="Org.Codehaus.Jackson.Jaxrs.JacksonJaxbJsonProvider"/>
	/// needed for deserialize JSON content to or
	/// serialize it from POJO objects.
	/// </summary>
	public class YarnJacksonJaxbJsonProvider : JacksonJaxbJsonProvider
	{
		public YarnJacksonJaxbJsonProvider()
			: base()
		{
		}

		public override ObjectMapper LocateMapper(Type type, MediaType mediaType)
		{
			ObjectMapper mapper = base.LocateMapper(type, mediaType);
			ConfigObjectMapper(mapper);
			return mapper;
		}

		public static void ConfigObjectMapper(ObjectMapper mapper)
		{
			AnnotationIntrospector introspector = new JaxbAnnotationIntrospector();
			mapper.SetAnnotationIntrospector(introspector);
			mapper.SetSerializationInclusion(JsonSerialize.Inclusion.NonNull);
		}
	}
}
